// [Modified File]: standarddagengine.java
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
 * @author ruifeng.wen (Refactored by Devin)
 */
@Slf4j
public class StandardDagEngine {

    private final StandardNodeExecutor nodeExecutor;
    private final Duration cacheTtl;
    private final int concurrencyLevel;

    // 构造函数不变...
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
        final String dagName = dagDefinition.getDagName();
        final String actualRequestId = (requestId != null && !requestId.trim().isEmpty()) ? requestId : generateRequestId();

        if (!dagDefinition.isInitialized()) {
            log.error("[RequestId: {}] DAG '{}' (上下文类型: {}) 尚未初始化，无法执行。",
                    actualRequestId, dagName, dagDefinition.getContextType().getSimpleName());
            throw new IllegalStateException(String.format("DAG '%s' is not initialized.", dagName));
        }

        log.debug("[RequestId: {}] 开始执行 DAG '{}' (上下文类型: {})",
                actualRequestId, dagName, dagDefinition.getContextType().getSimpleName());

        final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> requestCache = createRequestCache();

        List<String> nodeNames = dagDefinition.getExecutionOrder();
        if (nodeNames.isEmpty()) {
            if (!dagDefinition.getAllNodes().isEmpty()) {
                log.warn("[RequestId: {}] DAG '{}' 有节点但执行顺序为空，可能未初始化或初始化失败。", actualRequestId, dagName);
            } else {
                log.info("[RequestId: {}] DAG '{}' 为空，直接完成。", actualRequestId, dagName);
            }
            return Flux.empty();
        }

        // 使用 flatMap 并发处理节点，merge 合并事件流
        Flux<Event<?>> nodesEventFlux = Flux.fromIterable(nodeNames)
                .flatMap(nodeName -> processNodeAndGetEvents(nodeName, initialContext, requestCache, actualRequestId, dagDefinition),
                        this.concurrencyLevel);

        return mergeStreamsAndFinalize(nodesEventFlux, requestCache, actualRequestId, dagName);
    }

    private String generateRequestId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
    private <C> Cache<String, Mono<? extends NodeResult<C, ?, ?>>> createRequestCache() {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(1000);

        if (!cacheTtl.isNegative() && !cacheTtl.isZero()) {
            builder.expireAfterWrite(this.cacheTtl);
            log.trace("创建请求缓存，TTL: {}", cacheTtl);
        } else {
            log.trace("创建请求缓存，TTL 无效，缓存将立即过期或不生效");
            builder.expireAfterWrite(Duration.ZERO);
        }
        return builder.build();
    }


    /**
     * 处理单个节点：获取其执行 Mono，然后提取事件流。
     *
     * @return 该节点产生的事件流 Flux<Event<?>>
     */
    @SuppressWarnings("unchecked")
    private <C, P> Flux<Event<?>> processNodeAndGetEvents(
                                                           String nodeName,
                                                           C context,
                                                           Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
                                                           String requestId,
                                                           DagDefinition<C> dagDefinition) {

        // 1. 获取节点实例以确定其 Payload 类型 P
        DagNode<C, ?, ?> node = findNodeInstance(nodeName, dagDefinition, requestId);
        Class<?> nodePayloadType = node.getPayloadType();

        Mono<? extends NodeResult<C, ?, ?>> nodeResultMonoWildcard = nodeExecutor.getNodeExecutionMono(
                nodeName,
                (Class<P>) nodePayloadType,
                context,
                cache,
                dagDefinition,
                requestId
        );

        return nodeResultMonoWildcard
                .flatMapMany(result -> extractNodeEvents(result, nodeName, dagDefinition.getDagName(), requestId))
                .onErrorResume(e -> handleNodeStreamError(e, nodeName, dagDefinition.getDagName(), requestId));
    }
    /**
     * 从 DagDefinition 获取节点实例。
     */
    private <C> DagNode<C, ?, ?> findNodeInstance(
            String nodeName,
            DagDefinition<C> dagDefinition,
            String requestId) {

        return dagDefinition.getNodeAnyType(nodeName)
                .orElseThrow(() -> {
                    log.error("[RequestId: {}] DAG '{}': 在执行期间未找到节点 '{}' 实例。请检查 DAG 定义和执行顺序。",
                            requestId, dagDefinition.getDagName(), nodeName);
                    return new IllegalStateException(
                            String.format("在执行期间未找到节点 '%s' (DAG: '%s')",
                                    nodeName, dagDefinition.getDagName()));
                });
    }

    private <C> Flux<Event<?>> extractNodeEvents(
            NodeResult<C, ?, ?> result,
            String nodeName,
            String dagName,
            String requestId) {

        if (result.isFailure()) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行失败，其事件流将被跳过。错误: {}",
                    requestId, dagName, nodeName, result.getError().map(Throwable::getMessage).orElse("未知错误"));
            return Flux.empty();
        }
        if (result.isSkipped()) { // 处理跳过的情况
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 被跳过，其事件流将被跳过。",
                    requestId, dagName, nodeName);
            return Flux.empty();
        }

        @SuppressWarnings("unchecked")
        Flux<Event<?>> events = (Flux<Event<?>>) (Flux<? extends Event<?>>) result.getEvents();

        if (events == Flux.<Event<?>>empty()) { // 比较引用
            log.trace("[RequestId: {}] DAG '{}': 节点 '{}' 执行成功，但未产生事件。", requestId, dagName, nodeName);
        } else {
            log.trace("[RequestId: {}] DAG '{}': 节点 '{}' 执行成功，获取其事件流。", requestId, dagName, nodeName);
        }

        return events.doOnError(e -> log.error(
                "[RequestId: {}] DAG '{}': 节点 '{}' 的事件流处理中发生错误: {}",
                requestId, dagName, nodeName, e.getMessage(), e
        )).onErrorResume(e -> Flux.empty());
    }

    private Flux<Event<?>> handleNodeStreamError(
            Throwable e,
            String nodeName,
            String dagName,
            String requestId) {
        log.error("[RequestId: {}] DAG '{}': 处理节点 '{}' 获取事件流时发生意外错误: {}",
                requestId, dagName, nodeName, e.getMessage(), e);
        return Flux.empty();
    }

    private <C> Flux<Event<?>> mergeStreamsAndFinalize(
            Flux<Event<?>> nodesEventFlux,
            Cache<String, Mono<? extends NodeResult<C, ?, ?>>> requestCache,
            String requestId,
            String dagName) {

        return Flux.merge(nodesEventFlux)
                .doOnSubscribe(s -> log.debug("[RequestId: {}] DAG '{}' 事件流已订阅，开始接收节点事件。", requestId, dagName))
                .doOnNext(event -> log.trace("[RequestId: {}] DAG '{}' 收到事件: {}", requestId, dagName, event)) // 改为 trace
                .doOnComplete(() -> log.debug("[RequestId: {}] DAG '{}' 所有节点事件流处理完成。", requestId, dagName))
                .doOnError(e -> log.error("[RequestId: {}] DAG '{}' 合并事件流时发生错误: {}", // error 级别
                        requestId, dagName, e.getMessage(), e))
                .doFinally(signal -> cleanupCache(requestCache, requestId, dagName, signal));
    }

    private <C> void cleanupCache(
            Cache<String, Mono<? extends NodeResult<C, ?, ?>>> requestCache,
            String requestId,
            String dagName,
            SignalType signal) {

        long cacheSize = requestCache.estimatedSize();
        if (cacheSize > 0) {
            log.debug("[RequestId: {}] DAG '{}' 事件流终止 (信号: {}), 清理请求缓存 (大小: {}).",
                    requestId, dagName, signal, cacheSize);
            requestCache.invalidateAll();
            requestCache.cleanUp();
            log.debug("[RequestId: {}] DAG '{}' 请求缓存已清理.", requestId, dagName);
        } else {
            log.trace("[RequestId: {}] DAG '{}' 事件流终止 (信号: {}), 请求缓存为空，无需清理.", requestId, dagName, signal);
        }
    }
}
