package xyz.vvrf.reactor.dag.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 标准DAG执行引擎 - 负责协调节点执行并合并事件流。
 * **修改**: 使用 scanWith 按拓扑顺序执行节点，并累积所有节点结果，
 * 以支持间接数据依赖。
 *
 * @author ruifeng.wen (Refactored by Devin)
 */
@Slf4j
public class StandardDagEngine {

    private final StandardNodeExecutor nodeExecutor;
    private final Duration cacheTtl; // TTL for the request-level execution MONO cache

    // 移除 concurrencyLevel，因为 scanWith 通常是顺序的
    // private final int concurrencyLevel;

    public StandardDagEngine(StandardNodeExecutor nodeExecutor, Duration cacheTtl) {
        this.nodeExecutor = Objects.requireNonNull(nodeExecutor, "NodeExecutor 不能为空");
        this.cacheTtl = Objects.requireNonNull(cacheTtl, "Cache TTL 不能为空");
        // this.concurrencyLevel = 1; // Force sequential execution due to scanWith
        if (cacheTtl.isNegative() || cacheTtl.isZero()) {
            log.warn("StandardDagEngine 配置的 cacheTtl <= 0，节点执行 Mono 缓存将被禁用或立即过期: {}", cacheTtl);
        }
        log.info("StandardDagEngine 初始化完成，节点执行 Mono 缓存TTL: {}", this.cacheTtl);
    }

    /**
     * 内部类，用于在 scanWith 中传递状态
     */
    @Getter
    @AllArgsConstructor
    private static class ExecutionState<C> {
        private final C context;
        // 存储所有已完成节点的结果
        private final Map<String, NodeResult<C, ?, ?>> completedResults;
        // 存储当前步骤产生的事件流 (用于后续合并)
        private final Flux<Event<?>> currentEvents;
    }

    /**
     * 执行指定 DAG 定义并返回合并后的事件流。
     *
     * @param <C>            上下文类型
     * @param initialContext 初始上下文对象
     * @param requestId      请求的唯一标识符 (可选, 为 null 则自动生成)，用于日志和追踪
     * @param dagDefinition  要执行的 DAG 的定义 (必须已初始化)
     * @return 合并所有成功节点事件流的 Flux<Event<?>>
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

        log.info("[RequestId: {}] 开始执行 DAG '{}' (上下文类型: {})", // Changed to info
                actualRequestId, dagName, dagDefinition.getContextType().getSimpleName());

        // 请求级缓存，存储节点执行的 Mono<NodeResult>，避免重复创建执行逻辑
        final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> requestMonoCache = createRequestMonoCache();
        // 用于累积实际结果的 Map，将在 scanWith 中更新
        final Map<String, NodeResult<C, ?, ?>> accumulatedResults = new ConcurrentHashMap<>();

        List<String> nodeNamesInOrder = dagDefinition.getExecutionOrder();
        if (nodeNamesInOrder.isEmpty()) {
            if (!dagDefinition.getAllNodes().isEmpty()) {
                log.warn("[RequestId: {}] DAG '{}' 有节点但执行顺序为空，可能未初始化或初始化失败。", actualRequestId, dagName);
            } else {
                log.info("[RequestId: {}] DAG '{}' 为空，直接完成。", actualRequestId, dagName);
            }
            return Flux.empty();
        }

        // 初始状态
        ExecutionState<C> initialState = new ExecutionState<>(
                initialContext,
                Collections.emptyMap(), // 初始时没有已完成的结果
                Flux.empty()            // 初始时没有事件
        );

        // 使用 scanWith 顺序处理节点，累积状态
        Flux<ExecutionState<C>> executionStates = Flux.fromIterable(nodeNamesInOrder)
                .scanWith(
                        () -> initialState, // 初始状态提供者
                        (currentState, nodeName) -> {
                            // 对于每个节点，执行它并更新状态
                            log.debug("[RequestId: {}] DAG '{}': 处理节点 '{}' (当前状态有 {} 个结果)",
                                    actualRequestId, dagName, nodeName, currentState.getCompletedResults().size());

                            // 调用 NodeExecutor 获取/创建执行 Mono
                            // 注意：这里传递的是 currentState.getCompletedResults()
                            Mono<NodeResult<C, ?, ?>> nodeResultMono = nodeExecutor.getNodeExecutionMono(
                                    nodeName,
                                    currentState.getContext(),
                                    requestMonoCache, // 缓存 Mono 执行逻辑
                                    currentState.getCompletedResults(), // 传递当前所有已完成的结果
                                    dagDefinition,
                                    actualRequestId
                            );

                            // 当节点执行完成时，更新状态
                            return nodeResultMono.map(result -> {
                                // 创建新的结果 Map (不可变性)
                                Map<String, NodeResult<C, ?, ?>> nextResults = new HashMap<>(currentState.getCompletedResults());
                                nextResults.put(nodeName, result); // 添加当前节点的结果

                                // 提取当前节点的事件流 (仅成功时)
                                Flux<Event<?>> currentEvents = Flux.empty();
                                if (result.isSuccess()) {
                                    // 类型转换
                                    @SuppressWarnings("unchecked")
                                    Flux<Event<?>> events = (Flux<Event<?>>) (Flux<? extends Event<?>>) result.getEvents();
                                    currentEvents = events
                                            .doOnError(e -> log.error(
                                                    "[RequestId: {}] DAG '{}': 节点 '{}' 的事件流处理中发生错误: {}",
                                                    actualRequestId, dagName, nodeName, e.getMessage(), e
                                            )).onErrorResume(e -> Flux.empty()); // 忽略事件流错误，继续DAG
                                } else if (result.isFailure()) {
                                    log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行失败，其事件流将被跳过. Error: {}",
                                            actualRequestId, dagName, nodeName, result.getError().map(Throwable::getMessage).orElse("N/A"));
                                } else if (result.isSkipped()) {
                                    log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 被跳过，无事件产生.", actualRequestId, dagName, nodeName);
                                }

                                // 返回新的状态
                                return new ExecutionState<>(
                                        currentState.getContext(), // Context 通常不变
                                        Collections.unmodifiableMap(nextResults), // 更新后的结果集
                                        currentEvents // 当前节点产生的事件
                                );
                            }).onErrorResume(error -> {
                                // 如果获取或执行 nodeResultMono 本身失败 (理论上 executor 内部已处理)
                                log.error("[RequestId: {}] DAG '{}': 处理节点 '{}' 的 Mono 时发生意外错误，DAG 可能无法继续: {}",
                                        actualRequestId, dagName, nodeName, error.getMessage(), error);
                                // 返回一个包含错误状态的状态，或者直接让流失败？
                                // 为了健壮性，返回一个包含现有结果的状态，但没有新事件
                                return Mono.just(new ExecutionState<>(
                                        currentState.getContext(),
                                        currentState.getCompletedResults(), // 保持之前的状态
                                        Flux.error(error) // 传递错误信号
                                ));
                            });
                        }
                ).skip(1); // 跳过初始状态，只关心每个节点执行后的状态

        // 从每个状态中提取事件流并合并
        return executionStates
                .concatMap(ExecutionState::getCurrentEvents) // 使用 concatMap 保证事件顺序大致跟随节点执行顺序
                .doOnSubscribe(s -> log.debug("[RequestId: {}] DAG '{}' 最终事件流已订阅，开始接收所有节点事件。", actualRequestId, dagName))
                .doOnNext(event -> log.trace("[RequestId: {}] DAG '{}' 最终流收到事件: {}", actualRequestId, dagName, event))
                .doOnComplete(() -> log.info("[RequestId: {}] DAG '{}' 所有节点处理完成，最终事件流处理完成。", actualRequestId, dagName)) // Changed to info
                .doOnError(e -> log.error("[RequestId: {}] DAG '{}' 合并或处理事件流时发生错误: {}",
                        actualRequestId, dagName, e.getMessage(), e))
                .doFinally(signal -> cleanupCache(requestMonoCache, actualRequestId, dagName, signal)); // 清理 Mono 缓存
    }

    private String generateRequestId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    // 创建请求级 Mono 缓存
    private <C> Cache<String, Mono<? extends NodeResult<C, ?, ?>>> createRequestMonoCache() {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(1000); // 可配置

        if (!cacheTtl.isNegative() && !cacheTtl.isZero()) {
            builder.expireAfterWrite(this.cacheTtl);
            log.trace("创建请求级节点执行 Mono 缓存，TTL: {}", cacheTtl);
        } else {
            log.trace("创建请求级节点执行 Mono 缓存，TTL 无效，缓存将立即过期或不生效");
            builder.expireAfterWrite(Duration.ZERO); // 立即过期
        }
        // 类型转换是安全的，因为我们控制放入的内容
        @SuppressWarnings("unchecked")
        Cache<String, Mono<? extends NodeResult<C, ?, ?>>> builtCache = (Cache<String, Mono<? extends NodeResult<C, ?, ?>>>)(Cache<?, ?>)builder.build();
        return builtCache;
    }

    // 清理 Mono 缓存
    private <C> void cleanupCache(
            Cache<String, Mono<? extends NodeResult<C, ?, ?>>> requestMonoCache,
            String requestId,
            String dagName,
            SignalType signal) {

        long cacheSize = requestMonoCache.estimatedSize();
        if (cacheSize > 0) {
            log.debug("[RequestId: {}] DAG '{}' 最终事件流终止 (信号: {}), 清理节点执行 Mono 缓存 (大小: {}).",
                    requestId, dagName, signal, cacheSize);
            requestMonoCache.invalidateAll(); // 清除所有缓存条目
            requestMonoCache.cleanUp(); // 主动触发清理
            log.debug("[RequestId: {}] DAG '{}' 节点执行 Mono 缓存已清理.", requestId, dagName);
        } else {
            log.trace("[RequestId: {}] DAG '{}' 最终事件流终止 (信号: {}), 节点执行 Mono 缓存为空，无需清理.", requestId, dagName, signal);
        }
    }

    // 移除 processNodeAndGetEvents, findNodeInstance, extractNodeEvents, handleNodeStreamError, mergeStreamsAndFinalize
    // 因为逻辑已整合到 execute 方法的 scanWith 中
}
