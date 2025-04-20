package xyz.vvrf.reactor.dag.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * 标准DAG执行引擎 - 负责协调节点执行并合并事件流
 *
 * @author ruifeng.wen
 */
@Slf4j
public class StandardDagEngine {

    private final StandardNodeExecutor nodeExecutor;
    private final Duration cacheTtl;
    private final int concurrencyLevel; // 新增并发度配置

    /**
     * 创建标准DAG执行引擎
     *
     * @param nodeExecutor 节点执行器
     * @param cacheTtl     节点执行结果在请求级别缓存的生存时间
     * @param concurrencyLevel flatMap 操作的并发度 (例如，用于处理节点的数量)
     */
    public StandardDagEngine(StandardNodeExecutor nodeExecutor, Duration cacheTtl, int concurrencyLevel) {
        this.nodeExecutor = Objects.requireNonNull(nodeExecutor, "NodeExecutor 不能为空");
        this.cacheTtl = Objects.requireNonNull(cacheTtl, "Cache TTL 不能为空");
        if (concurrencyLevel <= 0) {
            throw new IllegalArgumentException("Concurrency level must be positive.");
        }
        this.concurrencyLevel = concurrencyLevel;
        if (cacheTtl.isNegative() || cacheTtl.isZero()) {
            log.warn("StandardDagEngine 配置的 cacheTtl <= 0，缓存将被禁用或立即过期: {}", cacheTtl);
        }
        log.info("StandardDagEngine 初始化完成，节点结果缓存TTL: {}, 并发度: {}", this.cacheTtl, this.concurrencyLevel);
    }

    public StandardDagEngine(StandardNodeExecutor nodeExecutor, Duration cacheTtl) {
        // 提供一个合理的默认值，或者仍使用 CPU 核心数
        this(nodeExecutor, cacheTtl, Math.max(1, Runtime.getRuntime().availableProcessors()));
    }

    /**
     * 执行指定 DAG 定义并返回合并后的事件流。
     *
     * @param <C>            上下文类型
     * @param initialContext 初始上下文对象
     * @param requestId      请求的唯一标识符 (可选, 为 null 则自动生成)，用于日志和追踪
     * @param dagDefinition  要执行的 DAG 的定义 (必须已初始化)
     * @return 合并所有节点事件流的 Flux<Event<?>>
     * @throws IllegalStateException 如果 DAG 定义未初始化
     */
    public <C> Flux<Event<?>> execute(
            final C initialContext,
            final String requestId,
            final DagDefinition<C> dagDefinition
    ) {
        // 确保 DAG 定义已初始化
        // 注意：这里假设外部调用者负责初始化。如果需要引擎强制初始化，可以在这里调用 dagDefinition.initializeIfNeeded()
        // 但这可能不是引擎的职责。暂时依赖外部保证。

        final String dagName = dagDefinition.getDagName();
        final String actualRequestId = (requestId != null && !requestId.trim().isEmpty()) ? requestId : generateRequestId();

        if (!dagDefinition.isInitialized()) {
            log.error("[RequestId: {}] DAG '{}' (上下文类型: {}) 尚未初始化，无法执行。",
                    actualRequestId, dagName, dagDefinition.getContextType().getSimpleName());
            // 或者抛出更具体的异常
            throw new IllegalStateException(String.format("DAG '%s' is not initialized.", dagName));
        }

        log.info("[RequestId: {}] 开始执行 DAG '{}' (上下文类型: {})",
                actualRequestId, dagName, dagDefinition.getContextType().getSimpleName());

        // 为当前请求创建节点结果缓存 (Mono<NodeResult>)
        // Key: nodeName#payloadTypeName, Value: Mono<? extends NodeResult<C, ?, ?>>
        final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> requestCache = createRequestCache();

        // 获取拓扑排序后的节点执行顺序
        List<String> nodeNames = dagDefinition.getExecutionOrder();
        if (nodeNames.isEmpty() && !dagDefinition.getAllNodes().isEmpty()) {
            log.warn("[RequestId: {}] DAG '{}' 有节点但执行顺序为空，可能未初始化或初始化失败。", actualRequestId, dagName);
            return Flux.empty(); // 或者抛出异常？
        }
        if (nodeNames.isEmpty()) {
            log.info("[RequestId: {}] DAG '{}' 为空或无执行顺序，直接完成。", actualRequestId, dagName);
            return Flux.empty();
        }


        // 创建所有节点的事件流的 Flux
        // flatMap 会按顺序订阅内部的 Mono/Flux，但内部 Mono/Flux 的执行可能是并发的（取决于调度器）
        // merge 会并发地合并所有源 Flux 发出的事件
        Flux<Event<?>> nodesEventFlux = Flux.fromIterable(nodeNames)
                .flatMap(nodeName -> processNodeAndGetEvents(nodeName, initialContext, requestCache, actualRequestId, dagDefinition),
                        this.concurrencyLevel);

        // 合并所有节点的事件流，并添加日志和清理逻辑
        return mergeStreamsAndFinalize(nodesEventFlux, requestCache, actualRequestId, dagName);
    }

    /**
     * 生成唯一请求ID
     */
    private String generateRequestId() {
        return UUID.randomUUID().toString().substring(0, 8); // 短一点的 UUID
    }

    /**
     * 创建请求级别的节点结果缓存
     */
    private <C> Cache<String, Mono<? extends NodeResult<C, ?, ?>>> createRequestCache() {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(1000); // 限制缓存大小防止内存泄漏

        if (!cacheTtl.isNegative() && !cacheTtl.isZero()) {
            builder.expireAfterWrite(this.cacheTtl);
            log.debug("创建请求缓存，TTL: {}", cacheTtl);
        } else {
            log.debug("创建请求缓存，TTL 无效，缓存将立即过期或不生效");
            builder.expireAfterWrite(Duration.ZERO); // 设为0使其几乎立即过期
        }

        return builder.build();
    }


    /**
     * 处理单个节点：获取其执行 Mono，然后提取事件流。
     *
     * @return 该节点产生的事件流 Flux<Event<?>>
     */
    private <C> Flux<Event<?>> processNodeAndGetEvents(
            String nodeName,
            C context,
            Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
            String requestId,
            DagDefinition<C> dagDefinition) {

        // 1. 找到节点实例 (不关心具体 Payload/Event 类型)
        DagNode<C, ?, ?> node = findNode(nodeName, dagDefinition, requestId);

        // 2. 获取节点的 Payload 类型 (用于调用执行器)
        //    注意：一个节点名可能对应多个实现（不同 Payload 类型），但执行顺序是按名称来的。
        //    这里我们假设执行器能够处理这种情况，或者 DAG 定义保证了名称的唯一性。
        //    如果一个名称对应多个 Payload 类型，我们需要决定执行哪一个。
        //    当前 StandardNodeExecutor 的 getNodeExecutionMono 需要指定 Payload 类型。
        //    这暗示了执行顺序列表中的名称应该唯一地标识一个执行单元（节点+其主要输出类型）。
        //    如果 AbstractDagDefinition 允许多个同名节点（不同 Payload 类型），这里的逻辑需要调整。
        //    假设：执行顺序中的 nodeName 对应一个明确的 DagNode 实例（通常是 nodesByName 中的那个）。
        Class<?> payloadType = node.getPayloadType(); // 获取此节点实例声明的 Payload 类型

        // 3. 获取节点的执行结果 Mono (会触发依赖解析和执行)
        Mono<? extends NodeResult<C, ?, ?>> nodeResultMono = nodeExecutor.getNodeExecutionMono(
                nodeName,
                payloadType, // 传递节点声明的 Payload 类型
                context,
                cache,
                dagDefinition,
                requestId
        );

        // 4. 从结果 Mono 中提取事件流
        return nodeResultMono
                .flatMapMany(result -> extractNodeEvents(result, nodeName, dagDefinition.getDagName(), requestId))
                .onErrorResume(e -> handleNodeStreamError(e, nodeName, dagDefinition.getDagName(), requestId));
    }

    /**
     * 从 DagDefinition 获取节点实例。
     */
    private <C> DagNode<C, ?, ?> findNode(
            String nodeName,
            DagDefinition<C> dagDefinition,
            String requestId) { // requestId 用于日志

        // 使用 getNodeAnyType 获取与名称匹配的节点实例
        return dagDefinition.getNodeAnyType(nodeName)
                .orElseThrow(() -> {
                    log.error("[RequestId: {}] DAG '{}': 在执行期间未找到节点 '{}'。请检查 DAG 定义和执行顺序。",
                            requestId, dagDefinition.getDagName(), nodeName);
                    return new IllegalStateException(
                            String.format("在执行期间未找到节点 '%s' (DAG: '%s')",
                                    nodeName, dagDefinition.getDagName()));
                });
    }

    /**
     * 从节点执行结果中安全地提取事件流。
     */
    private <C> Flux<Event<?>> extractNodeEvents(
            NodeResult<C, ?, ?> result, // 使用通配符，因为我们不关心 Payload 类型 P
            String nodeName,
            String dagName,
            String requestId) {

        if (result.isFailure()) {
            // 如果节点执行失败，记录错误并返回空流
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行失败，其事件流将被跳过。错误: {}",
                    requestId, dagName, nodeName, result.getError().map(Throwable::getMessage).orElse("未知错误"));
            return Flux.empty();
        }

        // 节点执行成功，获取事件流
        // result.getEvents() 返回 Flux<Event<T>>，我们需要 Flux<Event<?>>
        // 直接转换是类型安全的，因为 Event<?> 是 Event<T> 的超类型。
        @SuppressWarnings("unchecked") // 类型转换是安全的
        Flux<Event<?>> events = (Flux<Event<?>>) (Flux<? extends Event<?>>) result.getEvents();

        if (events == Flux.<Event<?>>empty()) {
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 执行成功，但未产生事件。", requestId, dagName, nodeName);
        } else {
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 执行成功，获取其事件流。", requestId, dagName, nodeName);
        }

        // 添加错误处理，以防事件流本身产生错误
        return events.doOnError(e -> log.error(
                "[RequestId: {}] DAG '{}': 节点 '{}' 的事件流处理中发生错误: {}",
                requestId, dagName, nodeName, e.getMessage(), e
        )).onErrorResume(e -> Flux.empty()); // 如果事件流出错，忽略该流的后续事件
    }

    /**
     * 处理获取节点事件流时发生的意外错误（例如 findNode 失败，或 nodeResultMono 本身失败）。
     */
    private Flux<Event<?>> handleNodeStreamError(
            Throwable e,
            String nodeName,
            String dagName,
            String requestId) {

        log.error("[RequestId: {}] DAG '{}': 处理节点 '{}' 获取事件流时发生意外错误: {}",
                requestId, dagName, nodeName, e.getMessage(), e);
        // 返回空流，避免中断整个 DAG 执行
        return Flux.empty();
    }

    /**
     * 合并所有节点的事件流，并添加最终的日志记录和缓存清理。
     */
    private <C> Flux<Event<?>> mergeStreamsAndFinalize(
            Flux<Event<?>> nodesEventFlux,
            Cache<String, Mono<? extends NodeResult<C, ?, ?>>> requestCache,
            String requestId,
            String dagName) {

        // 使用 merge 并发合并所有节点的事件流
        return Flux.merge(nodesEventFlux)
                .doOnSubscribe(s -> log.info("[RequestId: {}] DAG '{}' 事件流已订阅，开始接收节点事件。", requestId, dagName))
                .doOnNext(event -> log.trace("[RequestId: {}] DAG '{}' 收到事件: {}", requestId, dagName, event))
                .doOnComplete(() -> log.info("[RequestId: {}] DAG '{}' 所有节点事件流处理完成。", requestId, dagName))
                .doOnError(e -> log.error("[RequestId: {}] DAG '{}' 合并事件流时发生错误: {}",
                        requestId, dagName, e.getMessage(), e))
                // doFinally 在完成、错误或取消时都会执行
                .doFinally(signal -> cleanupCache(requestCache, requestId, dagName, signal));
    }

    /**
     * 清理请求级别的节点结果缓存。
     */
    private <C> void cleanupCache(
            Cache<String, Mono<? extends NodeResult<C, ?, ?>>> requestCache,
            String requestId,
            String dagName,
            SignalType signal) { // signal 可以是 onComplete, onError, cancel

        long cacheSize = requestCache.estimatedSize();
        log.info("[RequestId: {}] DAG '{}' 事件流终止 (信号: {}), 清理请求缓存 (当前大小: {}).",
                requestId, dagName, signal, cacheSize);
        // 使所有缓存条目失效
        requestCache.invalidateAll();
        // 主动触发清理（对于基于大小或时间的过期可能有用）
        requestCache.cleanUp();
        log.debug("[RequestId: {}] DAG '{}' 请求缓存已清理.", requestId, dagName);
    }

}
