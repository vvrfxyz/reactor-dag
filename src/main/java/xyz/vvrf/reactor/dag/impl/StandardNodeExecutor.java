// [文件名称]: StandardNodeExecutor.java
package xyz.vvrf.reactor.dag.impl;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import xyz.vvrf.reactor.dag.core.*;
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;

import java.time.Instant;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * 标准节点执行器 - 负责执行单个节点逻辑 (NodeLogic) 并处理依赖关系、条件边和缓存。
 * 使用 DagNodeDefinition 获取节点配置。
 * 返回 Mono<NodeResult>，其完成信号表示节点逻辑单元结束。
 * 节点间数据传递通过共享 Context C 进行。
 *
 * @author ruifeng.wen (refactored with conditional edges)
 */
@Slf4j
public class StandardNodeExecutor {

    private final Duration defaultNodeTimeout;
    private final Scheduler nodeExecutionScheduler;
    private final List<DagMonitorListener> monitorListeners; // 监听器列表

    /**
     * 使用指定的超时、调度器和监控监听器创建 StandardNodeExecutor。
     *
     * @param defaultNodeTimeout     节点的默认执行超时时间 (必需)。
     * @param nodeExecutionScheduler 执行节点逻辑的调度器 (必需)。
     * @param monitorListeners       用于监控事件的监听器列表 (可以为空，但不能为 null)。
     */
    public StandardNodeExecutor(Duration defaultNodeTimeout,
                                Scheduler nodeExecutionScheduler,
                                List<DagMonitorListener> monitorListeners) {
        this.defaultNodeTimeout = Objects.requireNonNull(defaultNodeTimeout, "默认节点超时时间不能为空");
        this.nodeExecutionScheduler = Objects.requireNonNull(nodeExecutionScheduler, "节点执行调度器不能为空");
        this.monitorListeners = Objects.requireNonNull(monitorListeners, "Monitor listeners 列表不能为空");

        if (defaultNodeTimeout.isNegative() || defaultNodeTimeout.isZero()) {
            log.warn("配置的 defaultNodeTimeout <= 0，节点执行可能不会超时: {}", defaultNodeTimeout);
        }

        log.info("StandardNodeExecutor 初始化完成，节点默认超时: {}, 调度器: {}, 监听器数量: {}",
                defaultNodeTimeout, nodeExecutionScheduler, monitorListeners.size());
    }

    /**
     * 获取或创建节点执行的 Mono<NodeResult>，支持请求级缓存。
     *
     * @param <C>           上下文类型
     * @param nodeName      节点名称
     * @param context       当前上下文 (必须是线程安全的)
     * @param cache         请求级缓存 (Key: String (nodeName), Value: Mono<NodeResult<C, ?>>)
     * @param dagDefinition DAG 定义
     * @param requestId     请求 ID，用于日志
     * @return 返回一个 Mono，该 Mono 在订阅时会执行节点或从缓存返回结果。
     *         结果 NodeResult 的事件类型为通配符 ?。
     */
    public <C> Mono<NodeResult<C, ?>> getNodeExecutionMono(
            final String nodeName,
            final C context,
            final Cache<String, Mono<NodeResult<C, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String cacheKey = nodeName; // Key 直接用节点名
        final String dagName = dagDefinition.getDagName();

        // 使用 computeIfAbsent 简化缓存逻辑
        return Mono.defer(() -> {
            // 尝试从缓存获取 Mono<NodeResult<C, ?>>
            Mono<NodeResult<C, ?>> cachedMono = cache.getIfPresent(cacheKey);

            if (cachedMono != null) {
                log.trace("[RequestId: {}] DAG '{}': 缓存命中节点 '{}'",
                        requestId, dagName, nodeName);
                return cachedMono; // 直接返回缓存的 Mono
            } else {
                log.debug("[RequestId: {}] DAG '{}': 缓存未命中，创建节点 '{}' 执行 Mono",
                        requestId, dagName, nodeName);

                // 创建新的执行 Mono
                Mono<NodeResult<C, ?>> newMono = createNodeExecutionMono(
                        nodeName, context, cache, dagDefinition, requestId);

                // 使用 .cache() 操作符确保 Mono 只执行一次并将结果缓存起来
                Mono<NodeResult<C, ?>> monoToCache = newMono.cache();
                cache.put(cacheKey, monoToCache); // 存入缓存

                return monoToCache; // 返回缓存的 Mono
            }
        });
    }


    /**
     * 创建节点执行的 Mono。
     * 这个 Mono 在订阅时会查找节点定义、解析依赖、检查所有入边的条件、执行节点逻辑。
     *
     * @return Mono<NodeResult < C, ?>> 事件类型 T 由节点逻辑决定，用 ? 表示
     */
    private <C> Mono<NodeResult<C, ?>> createNodeExecutionMono(
            final String nodeName,
            final C context,
            final Cache<String, Mono<NodeResult<C, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();

        // 使用 defer 确保每次订阅时都执行查找和依赖解析
        return Mono.defer(() -> {
            // 1. 查找节点定义
            DagNodeDefinition<C, ?> nodeDefinition;
            try {
                nodeDefinition = findNodeDefinition(nodeName, dagDefinition, requestId);
            } catch (Exception e) {
                log.error("[RequestId: {}] DAG '{}': 执行前查找节点定义 '{}' 失败。", requestId, dagName, nodeName, e);
                return Mono.error(e); // 直接返回错误
            }

            // 确定超时和重试策略
            Duration timeout = determineNodeTimeout(nodeDefinition);
            Retry retrySpec = nodeDefinition.getEffectiveRetrySpec();
            Class<?> eventType = nodeDefinition.getEventType(); // 获取事件类型用于后续创建结果

            // 记录开始时间并通知监听器
            Instant startTime = Instant.now();
            safeNotifyListeners(l -> l.onNodeStart(requestId, dagName, nodeName, nodeDefinition));

            log.debug("[RequestId: {}] DAG '{}': 准备执行节点 '{}' (逻辑: {}, 事件类型: {}), 超时: {}, 重试: {}",
                    requestId, dagName, nodeName, nodeDefinition.getNodeLogic().getLogicIdentifier(),
                    eventType.getSimpleName(), timeout, retrySpec != null);

            // 2. 解析依赖，获取包含依赖结果 Map 的 Mono
            Mono<Map<String, NodeResult<C, ?>>> dependenciesMono = resolveDependencies(
                    nodeDefinition, context, cache, dagDefinition, requestId);

            // 3. 依赖解析完成后，检查所有入边条件，然后决定是否执行节点逻辑
            return dependenciesMono
                    .flatMap(dependencyResults -> { // 依赖结果 Map
                        boolean allConditionsMet;
                        try {
                            // 检查所有入边的条件
                            allConditionsMet = checkAllEdgeConditions(nodeDefinition, context, dependencyResults, requestId, dagName);
                        } catch (Exception e) {
                            // 条件评估本身出错
                            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 评估入边条件时抛出异常，将视为不执行并产生错误结果。",
                                    requestId, dagName, nodeName, e);
                            Duration totalDuration = Duration.between(startTime, Instant.now());
                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, Duration.ZERO, e, nodeDefinition));
                            // 创建失败结果
                            return Mono.just(NodeResult.failure(context,
                                    new IllegalStateException("节点 " + nodeName + " 评估边条件失败", e),
                                    eventType));
                        }

                        if (allConditionsMet) {
                            // 所有条件满足，执行节点逻辑 (包含重试和超时)
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 所有入边条件满足，将执行。", requestId, dagName, nodeName);
                            Mono<NodeResult<C, ?>> resultMono = executeNodeLogicInternal(
                                    nodeDefinition, context, timeout, retrySpec, requestId, dagName, startTime);
                            return resultMono;
                        } else {
                            // 条件不满足，跳过执行
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 因至少一个入边条件不满足，将被跳过。", requestId, dagName, nodeName);
                            safeNotifyListeners(l -> l.onNodeSkipped(requestId, dagName, nodeName, nodeDefinition));
                            // 创建跳过结果
                            NodeResult<C, ?> skippedResult = NodeResult.skipped(context, eventType);
                            return Mono.just(skippedResult);
                        }
                    })
                    // 添加顶层错误处理，捕获依赖解析失败等情况
                    .onErrorResume(error -> {
                        log.error("[RequestId: {}] DAG '{}': 节点 '{}' 在执行前或依赖解析中失败: {}", requestId, dagName, nodeName, error.getMessage(), error);
                        Duration totalDuration = Duration.between(startTime, Instant.now());
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, Duration.ZERO, error, nodeDefinition));
                        // 创建失败结果
                        return Mono.just(NodeResult.failure(context, error, eventType));
                    });
        });
    }

    /**
     * 检查节点的所有入边条件是否都满足。
     *
     * @param nodeDefinition    当前节点定义
     * @param context           上下文
     * @param dependencyResults 依赖节点的结果 Map
     * @param requestId         请求 ID
     * @param dagName           DAG 名称
     * @return 如果所有条件都满足，则返回 true；否则返回 false。
     * @throws RuntimeException 如果任何一个条件的 Predicate 执行时抛出异常。
     */
    private <C> boolean checkAllEdgeConditions(
            DagNodeDefinition<C, ?> nodeDefinition,
            C context,
            Map<String, NodeResult<C, ?>> dependencyResults,
            String requestId,
            String dagName) {

        String nodeName = nodeDefinition.getNodeName();
        List<EdgeDefinition<C>> edges = nodeDefinition.getIncomingEdges();

        if (edges.isEmpty()) {
            log.trace("[RequestId: {}] DAG '{}': 节点 '{}' 无入边，默认条件满足。", requestId, dagName, nodeName);
            return true; // 没有依赖边，默认执行
        }

        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 正在检查 {} 条入边条件...", requestId, dagName, nodeName, edges.size());

        for (EdgeDefinition<C> edge : edges) {
            String depName = edge.getDependencyNodeName();
            NodeResult<C, ?> depResult = dependencyResults.get(depName);

            // 防御性检查：理论上 resolveDependencies 会确保所有依赖都有结果，除非 DAG 定义错误
            if (depResult == null) {
                log.error("[RequestId: {}] DAG '{}': 节点 '{}' 检查条件时未找到依赖 '{}' 的结果！这通常表示 DAG 定义或执行逻辑有误。",
                        requestId, dagName, nodeName, depName);
                // 视为条件不满足，或者可以抛出更严重的错误
                return false;
            }

            // 调用 EdgeDefinition 的 evaluateCondition 方法
            boolean conditionMet = edge.evaluateCondition(context, depResult);

            if (!conditionMet) {
                log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 的入边条件 (来自 '{}') 未满足 (依赖结果: {}).",
                        requestId, dagName, nodeName, depName, depResult.getStatus());
                return false; // 只要有一个条件不满足，就整体不满足
            } else {
                log.trace("[RequestId: {}] DAG '{}': 节点 '{}' 的入边条件 (来自 '{}') 满足.",
                        requestId, dagName, nodeName, depName);
            }
        }

        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 的所有 {} 条入边条件均满足。", requestId, dagName, nodeName, edges.size());
        return true; // 所有条件都检查完毕且都满足
    }


    /**
     * 查找节点定义实例。
     */
    private <C> DagNodeDefinition<C, ?> findNodeDefinition(
            String nodeName,
            DagDefinition<C> dagDefinition,
            String requestId) {
        return dagDefinition.getNodeDefinition(nodeName)
                .orElseThrow(() -> {
                    String dagName = dagDefinition.getDagName();
                    String errorMsg = String.format("节点定义 '%s' 在 DAG '%s' 中不存在。", nodeName, dagName);
                    log.error("[RequestId: {}] {}", requestId, errorMsg);
                    return new IllegalStateException(errorMsg);
                });
    }

    /**
     * 确定节点的实际执行超时时间。
     */
    private <C> Duration determineNodeTimeout(DagNodeDefinition<C, ?> nodeDefinition) {
        Duration nodeTimeout = nodeDefinition.getEffectiveExecutionTimeout();
        if (nodeTimeout != null && !nodeTimeout.isZero() && !nodeTimeout.isNegative()) {
            return nodeTimeout;
        }
        return defaultNodeTimeout;
    }

    /**
     * 解析节点的所有依赖。
     * 返回一个 Mono，该 Mono 完成时会发出一个 Map，包含所有依赖节点的名称及其对应的 NodeResult。
     * 这个 Mono 会等待所有依赖节点的 getNodeExecutionMono() 返回的 Mono 完成。
     */
    private <C> Mono<Map<String, NodeResult<C, ?>>> resolveDependencies(
            final DagNodeDefinition<C, ?> nodeDefinition,
            final C context,
            final Cache<String, Mono<NodeResult<C, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String nodeName = nodeDefinition.getNodeName();
        final String dagName = dagDefinition.getDagName();
        // 从边定义中获取所有依赖的名称
        final List<String> dependencyNames = nodeDefinition.getDependencyNames(); // 使用 getDependencyNames()

        if (dependencyNames.isEmpty()) {
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 无依赖。", requestId, dagName, nodeName);
            return Mono.just(Collections.emptyMap());
        }

        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 正在解析 {} 个依赖: {}",
                requestId, dagName, nodeName, dependencyNames.size(), dependencyNames);

        // 为每个依赖创建一个获取其 NodeResult 的 Mono<Map.Entry>
        List<Mono<Map.Entry<String, NodeResult<C, ?>>>> dependencyMonos = dependencyNames.stream()
                .map(depName -> resolveSingleDependency(depName, context, cache, dagDefinition, nodeName, requestId))
                .collect(Collectors.toList());

        // 使用 Flux.flatMap 并发执行所有依赖解析 Mono
        return Flux.fromIterable(dependencyMonos)
                .flatMap(mono -> mono) // 触发每个 Mono<Map.Entry> 的执行
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .doOnSuccess(results -> log.debug(
                        "[RequestId: {}] DAG '{}': 节点 '{}' 的所有 {} 个依赖的 NodeResult Mono 已完成，结果已就绪。",
                        requestId, dagName, nodeName, results.size()))
                .doOnError(e -> log.error(
                        "[RequestId: {}] DAG '{}': 节点 '{}' 解析依赖失败: {}",
                        requestId, dagName, nodeName, e.getMessage(), e));
    }

    /**
     * 解析单个依赖：递归调用 getNodeExecutionMono 获取依赖节点的 Mono<NodeResult>，
     * 并在该 Mono 完成后，将其结果包装成 Map.Entry。
     */
    private <C> Mono<Map.Entry<String, NodeResult<C, ?>>> resolveSingleDependency(
            String depName,
            C context,
            Cache<String, Mono<NodeResult<C, ?>>> cache,
            DagDefinition<C> dagDefinition,
            String dependentNodeName, // 请求依赖的节点名，用于日志
            String requestId) {

        String dagName = dagDefinition.getDagName();

        log.trace("[RequestId: {}] DAG '{}': 节点 '{}' 开始解析依赖 '{}'",
                requestId, dagName, dependentNodeName, depName);

        // 获取依赖节点的 Mono<NodeResult<C, ?>>
        Mono<NodeResult<C, ?>> dependencyResultMono = getNodeExecutionMono(
                depName, context, cache, dagDefinition, requestId);

        // 当依赖节点的 Mono<NodeResult> 完成时，处理结果
        return dependencyResultMono
                .map(result -> {
                    // 依赖节点的逻辑单元已完成，记录其最终状态
                    if (result.isFailure()) {
                        log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 的依赖 '{}' 执行失败，将使用此失败结果。错误: {}",
                                requestId, dagName, dependentNodeName, depName,
                                result.getError().map(Throwable::getMessage).orElse("未知错误"));
                    } else if (result.isSkipped()) {
                        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 的依赖 '{}' 被跳过。",
                                requestId, dagName, dependentNodeName, depName);
                    } else {
                        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 的依赖 '{}' 执行成功。",
                                requestId, dagName, dependentNodeName, depName);
                    }
                    // 创建 Map Entry
                    return new AbstractMap.SimpleEntry<>(depName, result);
                });
    }


    /**
     * 内部方法：实际执行节点逻辑 (NodeLogic)。
     * 返回 Mono<NodeResult<C, T>>，其中 T 是由节点逻辑决定的具体事件类型。
     */
    private <C, T> Mono<NodeResult<C, ?>> executeNodeLogicInternal(
            DagNodeDefinition<C, T> nodeDefinition, // 使用类型 T
            C context,
            Duration timeout,
            Retry retrySpec,
            String requestId,
            String dagName,
            Instant startTime) { // startTime 是 onNodeStart 的时间

        String nodeName = nodeDefinition.getNodeName();
        NodeLogic<C, T> nodeLogic = nodeDefinition.getNodeLogic();
        Class<T> eventType = nodeDefinition.getEventType();

        return Mono.deferContextual(contextView -> {
                    Instant logicStartTime = Instant.now();
                    log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (逻辑: {}) 核心逻辑开始执行...",
                            requestId, dagName, nodeName, nodeLogic.getLogicIdentifier());

                    return nodeLogic.execute(context)
                            .subscribeOn(nodeExecutionScheduler)
                            .timeout(timeout)
                            .doOnEach(signal -> {
                                signal.getContextView().<Instant>getOrEmpty("logicStartTime")
                                        .ifPresent(lStartTime -> {
                                            Instant endTime = Instant.now();
                                            Duration totalDuration = Duration.between(startTime, endTime);
                                            Duration logicDuration = Duration.between(lStartTime, endTime);

                                            if (signal.isOnNext()) {
                                                NodeResult<C, ?> result = (NodeResult<C, ?>) signal.get();
                                                logResult(result, nodeName, dagName, requestId);
                                                if (result.isSuccess()) {
                                                    safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagName, nodeName, totalDuration, logicDuration, result, nodeDefinition));
                                                } else {
                                                    Throwable err = result.getError().orElse(new RuntimeException("NodeResult 在 onNext 中指示失败但无错误对象"));
                                                    log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 在 doOnEach 的 onNext 中收到失败结果。", requestId, dagName, nodeName);
                                                    safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, err, nodeDefinition));
                                                }
                                            } else if (signal.isOnError()) {
                                                Throwable error = signal.getThrowable();
                                                if (error instanceof TimeoutException) {
                                                    log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试超时 ({}).", requestId, dagName, nodeName, timeout);
                                                    safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagName, nodeName, timeout, nodeDefinition));
                                                } else {
                                                    log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试失败: {}", requestId, dagName, nodeName, error.getMessage());
                                                }
                                            } else if (signal.isOnComplete()) {
                                                log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 的 Mono<NodeResult> 执行流完成。", requestId, dagName, nodeName);
                                            }
                                        });
                            })
                            .retryWhen(retrySpec != null ? retrySpec : Retry.max(0))
                            .onErrorResume(error -> Mono.deferContextual(errorContextView -> {
                                Instant endTime = Instant.now();
                                Instant lStartTime = errorContextView.<Instant>getOrDefault("logicStartTime", startTime);
                                Duration totalDuration = Duration.between(startTime, endTime);
                                Duration logicDuration = Duration.between(lStartTime, endTime);

                                log.error("[RequestId: {}] DAG '{}': 节点 '{}' 执行最终失败: {}",
                                        requestId, dagName, nodeName, error.getMessage(), error);
                                safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, error, nodeDefinition));
                                return Mono.just(NodeResult.failure(context, error, eventType));
                            }))
                            .contextWrite(ctx -> ctx.put("logicStartTime", logicStartTime));
                })
                .map(result -> (NodeResult<C, ?>) result); // 类型转换
    }


    /**
     * 记录 NodeResult 的日志。
     */
    private <C> void logResult(
            NodeResult<C, ?> result,
            String nodeName,
            String dagName,
            String requestId) {

        if (result.isSuccess()) {
            boolean hasEvents = result.getEvents() != (Flux<?>)Flux.empty();
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (事件类型: {}) 执行成功. HasEvents: {}",
                    requestId, dagName, nodeName, result.getEventType().getSimpleName(),
                    hasEvents ? "Yes" : "No");
        } else if (result.isFailure()) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' (事件类型: {}) 执行完成但返回失败: {}",
                    requestId, dagName, nodeName, result.getEventType().getSimpleName(),
                    result.getError().map(Throwable::getMessage).orElse("未知错误"));
        } else { // Skipped (理论上 execute 不应返回 Skipped, 但条件不满足时会创建 Skipped)
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (事件类型: {}) 被跳过或返回了 SKIPPED 状态", // 改为 debug
                    requestId, dagName, nodeName, result.getEventType().getSimpleName());
        }
    }

    /**
     * 安全地通知所有注册的监听器，捕获单个监听器的异常。
     */
    private void safeNotifyListeners(java.util.function.Consumer<DagMonitorListener> notification) {
        if (monitorListeners.isEmpty()) {
            return;
        }
        for (DagMonitorListener listener : monitorListeners) {
            try {
                notification.accept(listener);
            } catch (Exception e) {
                log.error("DAG Monitor Listener {} 抛出异常: {}", listener.getClass().getName(), e.getMessage(), e);
            }
        }
    }
}
