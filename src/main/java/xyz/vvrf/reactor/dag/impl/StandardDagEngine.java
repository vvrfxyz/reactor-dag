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

    /**
     * 创建标准DAG执行引擎
     *
     * @param nodeExecutor 节点执行器
     * @param cacheTtl 缓存生存时间
     */
    public StandardDagEngine(StandardNodeExecutor nodeExecutor, Duration cacheTtl) {
        this.nodeExecutor = nodeExecutor;
        this.cacheTtl = cacheTtl;
        log.info("StandardDagEngine 初始化完成，缓存TTL: {}", this.cacheTtl);
    }

    /**
     * 执行指定 DAG 定义并返回合并后的事件流。
     *
     * @param <C>            上下文类型
     * @param initialContext 初始上下文对象
     * @param requestId      请求的唯一标识符，用于日志和追踪
     * @param dagDefinition  要执行的 DAG 的定义
     * @return 合并所有节点事件流的 Flux<Event<?>>
     */
    public <C> Flux<Event<?>> execute(
            final C initialContext,
            final String requestId,
            final DagDefinition<C> dagDefinition
    ) {
        final String dagName = dagDefinition.getDagName();
        final String actualRequestId = requestId != null ? requestId : generateRequestId();

        log.info("[RequestId: {}] 开始执行 DAG '{}' (上下文类型: {})",
                actualRequestId, dagName, dagDefinition.getContextType().getSimpleName());

        // 为当前请求创建缓存
        final Cache<String, Mono<? extends NodeResult<C, ?>>> requestCache = createRequestCache();

        // 获取执行顺序
        List<String> nodeNames = dagDefinition.getExecutionOrder();

        // 创建所有节点的事件流
        Flux<Event<?>> nodesFlux = createNodesFlux(
                nodeNames, initialContext, requestCache, actualRequestId, dagDefinition);

        // 合并流并添加结束事件
        return mergeStreamsAndFinalize(nodesFlux, requestCache, actualRequestId, dagName);
    }

    /**
     * 生成唯一请求ID
     */
    private String generateRequestId() {
        return UUID.randomUUID().toString();
    }

    /**
     * 创建请求级别的缓存
     */
    private <C> Cache<String, Mono<? extends NodeResult<C, ?>>> createRequestCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(this.cacheTtl)
                .maximumSize(1000)
                .build();
    }

    /**
     * 创建所有节点的事件流
     */
    private <C> Flux<Event<?>> createNodesFlux(
            List<String> nodeNames,
            C context,
            Cache<String, Mono<? extends NodeResult<C, ?>>> cache,
            String requestId,
            DagDefinition<C> dagDefinition) {

        return Flux.fromIterable(nodeNames)
                .flatMap(nodeName -> processNode(nodeName, context, cache, requestId, dagDefinition));
    }

    /**
     * 处理单个节点并获取其事件流
     */
    private <C> Flux<Event<?>> processNode(
            String nodeName,
            C context,
            Cache<String, Mono<? extends NodeResult<C, ?>>> cache,
            String requestId,
            DagDefinition<C> dagDefinition) {

        DagNode<C, ?> node = findNode(nodeName, dagDefinition, requestId);
        Class<?> payloadType = node.getPayloadType();

        return nodeExecutor.getNodeExecutionMono(
                        nodeName,
                        payloadType,
                        context,
                        cache,
                        dagDefinition,
                        requestId
                )
                .flatMapMany(result -> extractNodeEvents(result, nodeName, dagDefinition.getDagName(), requestId))
                .onErrorResume(e -> handleNodeStreamError(e, nodeName, dagDefinition.getDagName(), requestId));
    }

    /**
     * 从 DagDefinition 获取节点
     */
    private <C> DagNode<C, ?> findNode(
            String nodeName,
            DagDefinition<C> dagDefinition,
            String requestId) {

        return dagDefinition.getNodeAnyType(nodeName)
                .orElseThrow(() -> new IllegalStateException(
                        String.format("在执行期间未找到节点 '%s' (DAG: '%s')",
                                nodeName, dagDefinition.getDagName())));
    }

    /**
     * 提取节点执行结果中的事件流
     */
    private <C, T> Flux<Event<?>> extractNodeEvents(
            NodeResult<C, T> result,
            String nodeName,
            String dagName,
            String requestId) {

        if (result.getError().isPresent()) {
            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 执行失败，其事件流将被跳过: {}",
                    requestId, dagName, nodeName, result.getError().get().getMessage());
            return Flux.empty();
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        Flux<Event<?>> events = (Flux) result.getEvents();
        return events;
    }

    /**
     * 处理节点事件流错误
     */
    private Flux<Event<?>> handleNodeStreamError(
            Throwable e,
            String nodeName,
            String dagName,
            String requestId) {

        log.error("[RequestId: {}] DAG '{}': 处理节点 '{}' 的事件流时发生意外错误: {}",
                requestId, dagName, nodeName, e.getMessage(), e);
        return Flux.empty();
    }

    /**
     * 合并所有节点的流并添加结束事件
     */
    private <C> Flux<Event<?>> mergeStreamsAndFinalize(
            Flux<Event<?>> nodesFlux,
            Cache<String, Mono<? extends NodeResult<C, ?>>> requestCache,
            String requestId,
            String dagName) {

        return Flux.merge(nodesFlux)
                .doOnSubscribe(s -> log.info("[RequestId: {}] DAG '{}' 事件流已订阅", requestId, dagName))
                .doOnComplete(() -> log.info("[RequestId: {}] DAG '{}' 事件流完成", requestId, dagName))
                .doOnError(e -> log.error("[RequestId: {}] DAG '{}' 事件流处理中发生错误: {}",
                        requestId, dagName, e.getMessage(), e))
                .doFinally(signal -> cleanupCache(requestCache, requestId, dagName, signal));
    }

    /**
     * 清理请求缓存
     */
    private <C> void cleanupCache(
            Cache<String, Mono<? extends NodeResult<C, ?>>> requestCache,
            String requestId,
            String dagName,
            SignalType signal) {

        log.info("[RequestId: {}] DAG '{}' 事件流终止 (信号: {}), 清理请求缓存.",
                requestId, dagName, signal);
        requestCache.invalidateAll();
        requestCache.cleanUp();
    }

}