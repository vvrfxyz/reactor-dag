// [文件名称]: StandardDagEngine.java
// 基本逻辑不变，仍然是协调 NodeExecutor 并合并最终的 Event 流。
package xyz.vvrf.reactor.dag.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import xyz.vvrf.reactor.dag.core.DagDefinition;
// import xyz.vvrf.reactor.dag.core.DagNodeDefinition; // 不直接使用
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * 标准DAG执行引擎 - 负责协调节点执行并合并最终输出的事件流。
 * 使用 StandardNodeExecutor 执行节点逻辑。
 *
 * @author ruifeng.wen (refactored)
 */
@Slf4j
public class StandardDagEngine {

    private final StandardNodeExecutor nodeExecutor;
    private final Duration cacheTtl;
    private final int concurrencyLevel; // flatMap 并发度

    /**
     * 创建标准DAG执行引擎
     *
     * @param nodeExecutor     节点执行器
     * @param cacheTtl         节点执行结果 Mono 在请求级别缓存的生存时间
     * @param concurrencyLevel flatMap 操作的并发度 (例如，用于处理节点的数量)
     */
    public StandardDagEngine(StandardNodeExecutor nodeExecutor, Duration cacheTtl, int concurrencyLevel) {
        this.nodeExecutor = Objects.requireNonNull(nodeExecutor, "NodeExecutor 不能为空");
        this.cacheTtl = Objects.requireNonNull(cacheTtl, "Cache TTL 不能为空");
        if (concurrencyLevel <= 0) {
            throw new IllegalArgumentException("并发度必须为正数。");
        }
        this.concurrencyLevel = concurrencyLevel;
        if (cacheTtl.isNegative() || cacheTtl.isZero()) {
            log.warn("StandardDagEngine 配置的 cacheTtl <= 0，缓存将被禁用或立即过期: {}", cacheTtl);
        }
        log.info("StandardDagEngine 初始化完成，节点结果缓存TTL: {}, 并发度: {}", this.cacheTtl, this.concurrencyLevel);
    }

    /**
     * 执行指定 DAG 定义并返回合并后的事件流。
     *
     * @param <C>            上下文类型
     * @param initialContext 初始上下文对象 (必须保证线程安全或有效不可变性)
     * @param requestId      请求的唯一标识符 (可选, 为 null 则自动生成)，用于日志和追踪
     * @param dagDefinition  要执行的 DAG 的定义 (必须已初始化)
     * @return 合并所有节点最终输出事件流的 Flux<Event<?>>
     * @throws IllegalStateException 如果 DAG 定义未初始化
     */
    public <C> Flux<Event<?>> execute(
            final C initialContext,
            final String requestId,
            final DagDefinition<C> dagDefinition
    ) {
        final String dagName = dagDefinition.getDagName();
        final String actualRequestId = (requestId != null && !requestId.trim().isEmpty()) ? requestId : generateRequestId();

        // 1. 检查 DAG 是否初始化
        if (!dagDefinition.isInitialized()) {
            log.error("[RequestId: {}] DAG '{}' (上下文类型: {}) 尚未初始化，无法执行。",
                    actualRequestId, dagName, dagDefinition.getContextType().getSimpleName());
            throw new IllegalStateException(String.format("DAG '%s' 未初始化。", dagName));
        }

        log.debug("[RequestId: {}] 开始执行 DAG '{}' (上下文类型: {})",
                actualRequestId, dagName, dagDefinition.getContextType().getSimpleName());

        // 2. 创建请求级缓存 (Key: nodeName, Value: Mono<NodeResult<C, ?>>)
        final Cache<String, Mono<NodeResult<C, ?>>> requestCache = createRequestCache();

        // 3. 获取拓扑排序后的节点执行顺序
        List<String> nodeNames = dagDefinition.getExecutionOrder();
        if (nodeNames.isEmpty()) {
            if (dagDefinition.getAllNodeDefinitions().isEmpty()) {
                log.info("[RequestId: {}] DAG '{}' 为空，直接完成。", actualRequestId, dagName);
            } else {
                log.warn("[RequestId: {}] DAG '{}' 有节点定义但执行顺序为空，可能初始化失败或所有节点都不可达。", actualRequestId, dagName);
            }
            return Flux.empty();
        }

        // 4. 创建处理流程: 遍历执行顺序 -> 获取节点执行 Mono -> 提取事件流
        //    flatMap 用于并发（受 concurrencyLevel 限制）订阅和执行 getNodeExecutionMono
        //    merge 用于并发合并所有节点产生的事件流
        Flux<Event<?>> nodesEventFlux = Flux.fromIterable(nodeNames)
                .flatMap(nodeName -> processNodeAndGetEvents(
                                nodeName, initialContext, requestCache, actualRequestId, dagDefinition),
                        this.concurrencyLevel); // 控制并发执行的节点数量

        // 5. 合并事件流并添加最终处理逻辑 (日志、缓存清理)
        return mergeStreamsAndFinalize(nodesEventFlux, requestCache, actualRequestId, dagName);
    }

    /**
     * 生成唯一请求ID
     */
    private String generateRequestId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * 创建请求级别的节点结果缓存
     */
    private <C> Cache<String, Mono<NodeResult<C, ?>>> createRequestCache() {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(1000); // 限制缓存大小

        if (!cacheTtl.isNegative() && !cacheTtl.isZero()) {
            builder.expireAfterWrite(this.cacheTtl);
            log.trace("创建请求缓存，TTL: {}", cacheTtl); // 改为 trace 级别
        } else {
            log.debug("创建请求缓存，TTL 无效，缓存将立即过期或不生效");
            builder.expireAfterWrite(Duration.ZERO); // 设为0使其几乎立即过期
        }
        return builder.build();
    }

    /**
     * 处理单个节点：获取其执行 Mono，然后提取其最终输出的事件流。
     *
     * @return 该节点产生的事件流 Flux<Event<?>>
     */
    private <C> Flux<Event<?>> processNodeAndGetEvents(
            String nodeName,
            C context,
            Cache<String, Mono<NodeResult<C, ?>>> cache,
            String requestId,
            DagDefinition<C> dagDefinition) {

        // 1. 获取节点的执行结果 Mono (会触发依赖解析和执行, 或从缓存获取)
        //    这个 Mono 完成时，表示节点的逻辑单元已完成。
        //    Mono 发出的 NodeResult 对象包含了用于最终输出的事件流 Flux<Event<?>>。
        Mono<NodeResult<C, ?>> nodeResultMono = nodeExecutor.getNodeExecutionMono(
                nodeName,
                context,
                cache,
                dagDefinition,
                requestId
        );

        // 2. 从结果 Mono 中提取事件流
        //    flatMapMany 会在 nodeResultMono 发出 NodeResult 对象后立即订阅其内部的 events Flux。
        return nodeResultMono
                .flatMapMany(result -> extractNodeEvents(result, nodeName, dagDefinition.getDagName(), requestId))
                // 处理获取 NodeResult Mono 本身可能发生的错误 (例如节点查找失败)
                .onErrorResume(e -> handleNodeStreamError(e, nodeName, dagDefinition.getDagName(), requestId));
    }


    /**
     * 从节点执行结果中安全地提取用于最终输出的事件流。
     */
    private <C> Flux<Event<?>> extractNodeEvents(
            NodeResult<C, ?> result,
            String nodeName,
            String dagName,
            String requestId) {

        // 1. 检查节点执行状态，只有成功才提取事件
        if (!result.isSuccess()) {
            if (result.isFailure()) {
                log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 执行失败，跳过其事件流提取。错误: {}",
                        requestId, dagName, nodeName, result.getError().map(Throwable::getMessage).orElse("未知错误"));
            } else { // Skipped
                log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 被跳过，无事件流。",
                        requestId, dagName, nodeName);
            }
            return Flux.empty(); // 失败或跳过，返回空流
        }

        // 2. 节点执行成功，获取事件流
        // NodeResult.getEvents() 返回的是 Flux<Event<T>>，需要强制转换
        @SuppressWarnings("unchecked") // 类型擦除后 T 变为 ?，这里转换是安全的
        Flux<Event<?>> events = (Flux<Event<?>>) (Flux<?>) result.getEvents();

        // 3. 记录获取事件流的日志
        log.trace("[RequestId: {}] DAG '{}': 节点 '{}' 执行成功，获取其最终输出事件流。", requestId, dagName, nodeName);

        // 4. 添加错误处理，以防事件流本身产生错误
        return events.doOnError(e -> log.error(
                "[RequestId: {}] DAG '{}': 节点 '{}' 的事件流处理中发生错误: {}",
                requestId, dagName, nodeName, e.getMessage(), e
        )).onErrorResume(e -> {
            // 如果事件流本身出错，记录警告并返回空流，避免中断整个 DAG
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 的事件流因错误而终止，后续事件被忽略。", requestId, dagName, nodeName);
            return Flux.empty();
        });
    }

    /**
     * 处理获取节点事件流时发生的意外错误（例如 getNodeExecutionMono 失败）。
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
            Cache<String, Mono<NodeResult<C, ?>>> requestCache,
            String requestId,
            String dagName) {

        // 使用 merge 并发合并所有节点的事件流
        return Flux.merge(nodesEventFlux)
                .doOnSubscribe(s -> log.debug("[RequestId: {}] DAG '{}' 事件流已订阅，开始接收节点事件。", requestId, dagName))
                .doOnNext(event -> log.trace("[RequestId: {}] DAG '{}' 收到事件: {}", requestId, dagName, event)) // 改为 trace
                .doOnComplete(() -> log.debug("[RequestId: {}] DAG '{}' 所有节点事件流处理完成。", requestId, dagName))
                .doOnError(e -> log.error("[RequestId: {}] DAG '{}' 合并事件流时发生错误: {}", // Error 级别
                        requestId, dagName, e.getMessage(), e))
                // doFinally 在完成、错误或取消时都会执行
                .doFinally(signal -> cleanupCache(requestCache, requestId, dagName, signal));
    }

    /**
     * 清理请求级别的节点结果缓存。
     */
    private <C> void cleanupCache(
            Cache<String, Mono<NodeResult<C, ?>>> requestCache,
            String requestId,
            String dagName,
            SignalType signal) {

        long cacheSize = requestCache.estimatedSize();
        if (cacheSize > 0 || signal != SignalType.ON_COMPLETE) { // 只在缓存非空或非正常完成时打印日志
            log.debug("[RequestId: {}] DAG '{}' 事件流终止 (信号: {}), 清理请求缓存 (大小: {}).",
                    requestId, dagName, signal, cacheSize);
            requestCache.invalidateAll();
            requestCache.cleanUp(); // 主动触发清理
            log.debug("[RequestId: {}] DAG '{}' 请求缓存已清理.", requestId, dagName);
        } else {
            log.trace("[RequestId: {}] DAG '{}' 事件流正常完成，请求缓存为空或已过期，无需清理。", requestId, dagName);
        }
    }
}
