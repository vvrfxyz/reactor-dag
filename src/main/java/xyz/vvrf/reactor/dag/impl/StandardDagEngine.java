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
 * 以支持从任意已完成的上游节点获取数据依赖。
 *
 * @author ruifeng.wen (Refactored by Devin based on requirements)
 */
@Slf4j
public class StandardDagEngine {

    private final StandardNodeExecutor nodeExecutor;
    private final Duration cacheTtl;
//    private final int concurrencyLevel;

    public StandardDagEngine(StandardNodeExecutor nodeExecutor, Duration cacheTtl) {
        this.nodeExecutor = Objects.requireNonNull(nodeExecutor, "NodeExecutor 不能为空");
        this.cacheTtl = Objects.requireNonNull(cacheTtl, "Cache TTL 不能为空");
//        this.concurrencyLevel = 1;
        if (cacheTtl.isNegative() || cacheTtl.isZero()) {
            log.warn("StandardDagEngine 配置的 cacheTtl <= 0，节点执行 Mono 缓存将被禁用或立即过期: {}", cacheTtl);
        }
        log.info("StandardDagEngine 初始化完成，节点执行 Mono 缓存TTL: {}", this.cacheTtl);
    }

    /**
     * 内部类
     * 包含当前上下文和所有已完成节点的结果映射。
     */
    @Getter
    @AllArgsConstructor
    private static class ExpandState<C> {
        private final C context;
        // 存储所有已完成节点的结果 (不可变视图)
        private final Map<String, NodeResult<C, ?, ?>> completedResults;
        // 下一个要处理的节点在执行顺序列表中的索引
        private final int nextNodeIndex;
        // 上一个节点执行产生的事件
        private final Flux<Event<?>> eventsFromLastStep;
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

        log.info("[RequestId: {}] 开始执行 DAG '{}' (上下文类型: {})",
                actualRequestId, dagName, dagDefinition.getContextType().getSimpleName());

        // 请求级缓存
        final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> requestMonoCache = createRequestMonoCache(actualRequestId, dagName);

        final List<String> nodeNamesInOrder = dagDefinition.getExecutionOrder();
        if (nodeNamesInOrder.isEmpty()) {
            if (!dagDefinition.getAllNodes().isEmpty()) {
                log.warn("[RequestId: {}] DAG '{}' 有节点但执行顺序为空，可能未初始化或初始化失败。", actualRequestId, dagName);
            } else {
                log.info("[RequestId: {}] DAG '{}' 为空，直接完成。", actualRequestId, dagName);
            }
            return Flux.empty();
        }

        // 初始状态，准备处理第一个节点 (index 0)
        ExpandState<C> initialExpandState = new ExpandState<>(
                initialContext,
                Collections.emptyMap(),
                0, // 下一个处理 index 0
                Flux.empty() // 初始没有事件
        );

        // 使用 Mono.expand 来顺序执行异步步骤并传递状态
        Flux<ExpandState<C>> executionStatesFlux = Mono.just(initialExpandState)
                .expand(currentState -> {
                    // 检查是否还有节点需要处理
                    if (currentState.getNextNodeIndex() >= nodeNamesInOrder.size()) {
                        log.debug("[RequestId: {}] DAG '{}': 所有节点处理完毕，结束 expand。", actualRequestId, dagName);
                        return Mono.empty(); // 没有更多节点，终止展开
                    }

                    // 获取下一个要执行的节点名称
                    String nodeName = nodeNamesInOrder.get(currentState.getNextNodeIndex());
                    log.debug("[RequestId: {}] DAG '{}': expand - 处理节点 '{}' (Index: {}, 当前累积结果数: {})",
                            actualRequestId, dagName, nodeName, currentState.getNextNodeIndex(), currentState.getCompletedResults().size());

                    // 调用 NodeExecutor 获取/创建执行 Mono
                    // **关键**: 传递 currentState.getCompletedResults() 给 Executor
                    Mono<NodeResult<C, ?, ?>> nodeResultMono = nodeExecutor.getNodeExecutionMono(
                            nodeName,
                            currentState.getContext(),
                            requestMonoCache, // 缓存 Mono 执行逻辑
                            currentState.getCompletedResults(), // 传递当前所有已完成的结果
                            dagDefinition,
                            actualRequestId
                    );

                    // 当节点执行完成时 (无论成功失败)，计算下一个状态
                    return nodeResultMono.map(result -> {
                        // 创建新的结果 Map (基于当前状态，添加新结果)
                        Map<String, NodeResult<C, ?, ?>> nextResults = new HashMap<>(currentState.getCompletedResults());
                        nextResults.put(nodeName, result); // 添加当前节点的结果

                        // 提取当前节点的事件流 (仅成功时)
                        Flux<Event<?>> currentEvents = Flux.empty();
                        if (result.isSuccess()) {
                            @SuppressWarnings("unchecked")
                            Flux<Event<?>> events = (Flux<Event<?>>) (Flux<? extends Event<?>>) result.getEvents();
                            currentEvents = events
                                    .doOnError(e -> log.error(
                                            "[RequestId: {}] DAG '{}': 节点 '{}' 的事件流处理中发生错误: {}",
                                            actualRequestId, dagName, nodeName, e.getMessage(), e
                                    ))
                                    .onErrorResume(e -> {
                                        log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 事件流错误被忽略，继续执行。", actualRequestId, dagName, nodeName);
                                        return Flux.empty(); // 忽略事件流错误
                                    });
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 成功，提取其事件流。", actualRequestId, dagName, nodeName);
                        } else if (result.isFailure()) {
                            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行失败，其事件流将被跳过. Error: {}",
                                    actualRequestId, dagName, nodeName, result.getError().map(Throwable::getMessage).orElse("N/A"));
                        } else if (result.isSkipped()) {
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 被跳过，无事件产生.", actualRequestId, dagName, nodeName);
                        }

                        // 返回下一个状态，包含更新后的结果集、下一个节点索引和当前节点的事件
                        return new ExpandState<>(
                                currentState.getContext(),
                                Collections.unmodifiableMap(nextResults), // 更新后的结果集
                                currentState.getNextNodeIndex() + 1, // 准备处理下一个节点
                                currentEvents // 当前节点产生的事件
                        );
                    }).onErrorResume(error -> {
                        // 如果 getNodeExecutionMono 本身失败 (例如 NodeConfigurationException)
                        // 或者在 map 操作中出现意外错误
                        log.error("[RequestId: {}] DAG '{}': 处理节点 '{}' 时发生严重错误，终止 expand: {}",
                                actualRequestId, dagName, nodeName, error.getMessage(), error);
                        // 返回一个错误 Mono 以终止 expand 并将错误传播出去
                        return Mono.error(error);
                    });
                }); // end of expand lambda

        // 从每个状态中提取上一步产生的事件流并合并
        // expand 会发出所有状态，包括初始状态。我们需要跳过初始状态的空事件。
        return executionStatesFlux
                .skip(1) // 跳过 initialExpandState，因为它没有执行任何节点
                .concatMap(ExpandState::getEventsFromLastStep) // 使用 concatMap 保证事件顺序
                .doOnSubscribe(s -> log.debug("[RequestId: {}] DAG '{}' 最终事件流已订阅，开始接收所有节点事件。", actualRequestId, dagName))
                .doOnNext(event -> log.trace("[RequestId: {}] DAG '{}' 最终流收到事件: {}", actualRequestId, dagName, event))
                .doOnComplete(() -> log.info("[RequestId: {}] DAG '{}' 所有节点处理完成，最终事件流处理完成。", actualRequestId, dagName))
                .doOnError(e -> log.error("[RequestId: {}] DAG '{}' 合并或处理事件流时发生错误: {}",
                        actualRequestId, dagName, e.getMessage(), e))
                .doFinally(signal -> cleanupCache(requestMonoCache, actualRequestId, dagName, signal)); // 清理 Mono 缓存
    }

    private String generateRequestId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    // 创建请求级 Mono 缓存
    private <C> Cache<String, Mono<? extends NodeResult<C, ?, ?>>> createRequestMonoCache(String requestId, String dagName) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(1000); // 可配置

        if (!cacheTtl.isNegative() && !cacheTtl.isZero()) {
            builder.expireAfterWrite(this.cacheTtl);
            log.debug("[RequestId: {}] DAG '{}': 创建请求级节点执行 Mono 缓存，TTL: {}", requestId, dagName, cacheTtl);
        } else {
            log.debug("[RequestId: {}] DAG '{}': 创建请求级节点执行 Mono 缓存，TTL 无效，缓存将立即过期或不生效", requestId, dagName);
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
}
