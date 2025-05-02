// [file name]: StandardNodeExecutor.java
package xyz.vvrf.reactor.dag.impl;

import xyz.vvrf.reactor.dag.core.DependencyAccessor;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.core.*;
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener; // 引入监听器

import java.time.Instant;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * 标准节点执行器 - 负责执行单个节点逻辑 (NodeLogic) 并处理依赖关系和缓存。
 * 使用 DagNodeDefinition 获取节点配置。
 * 返回 Mono<NodeResult>，其完成信号表示节点逻辑单元结束。
 *
 * @author ruifeng.wen (refactored)
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
        this.monitorListeners = Objects.requireNonNull(monitorListeners, "Monitor listeners list cannot be null");

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
     * @param context       当前上下文
     * @param cache         请求级缓存 (Key: String (nodeName), Value: Mono<NodeResult<C, ?>>)
     * @param dagDefinition DAG 定义
     * @param requestId     请求 ID，用于日志
     * @return 返回一个 Mono，该 Mono 在订阅时会执行节点或从缓存返回结果。
     *         结果 NodeResult 的事件类型为通配符 ?。
     */
    public <C> Mono<NodeResult<C, ?>> getNodeExecutionMono(
            final String nodeName,
            final C context,
            final Cache<String, Mono<NodeResult<C, ?>>> cache, // 缓存的值类型改变
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
                Mono<NodeResult<C, ?>> monoToCache = newMono.cache(); // cache() 返回 Mono<T>
                cache.put(cacheKey, monoToCache); // 存入缓存

                return monoToCache; // 返回缓存的 Mono
            }
        });
    }


    /**
     * 创建节点执行的 Mono。
     * 这个 Mono 在订阅时会查找节点定义、解析依赖、执行节点逻辑。
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
                // 返回一个立即失败的 Mono，携带查找失败的错误
                // 需要知道 EventType 来创建 Failure NodeResult，但此时未知。
                // 方案1：抛出异常，让上层处理。
                // 方案2：创建一个特殊的 Error NodeResult？
                // 方案3：让 findNodeDefinition 返回 Optional，这里处理 empty 情况。
                // 采用方案1，直接返回 Mono.error
                return Mono.error(e);
            }

            // 确定超时和重试策略
            Duration timeout = determineNodeTimeout(nodeDefinition);
            Retry retrySpec = nodeDefinition.getEffectiveRetrySpec();
            Class<?> eventType = nodeDefinition.getEventType(); // 获取事件类型用于后续创建结果

            // 记录开始时间并通知监听器
            Instant startTime = Instant.now();
            safeNotifyListeners(l -> l.onNodeStart(requestId, dagName, nodeName, nodeDefinition)); // 传递 Definition

            log.debug("[RequestId: {}] DAG '{}': 准备执行节点 '{}' (逻辑: {}, 事件类型: {}), 超时: {}, 重试: {}",
                    requestId, dagName, nodeName, nodeDefinition.getNodeLogic().getLogicIdentifier(),
                    eventType.getSimpleName(), timeout, retrySpec != null);

            // 2. 解析依赖，获取包含依赖结果的 Map 的 Mono
            Mono<Map<String, NodeResult<C, ?>>> dependenciesMono = resolveDependencies(
                    nodeDefinition, context, cache, dagDefinition, requestId);

            // 3. 依赖解析完成后，检查是否执行，然后执行节点逻辑
            return dependenciesMono
                    .flatMap(dependencyResults -> {
                        DependencyAccessor<C> accessor = new DefaultDependencyAccessor<>(dependencyResults);

                        boolean shouldExec;
                        try {
                            // 使用节点定义中的 shouldExecute 判断
                            shouldExec = nodeDefinition.shouldExecute(context, accessor);
                        } catch (Exception e) {
                            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 的 shouldExecute 方法抛出异常，将视为不执行并产生错误结果。",
                                    requestId, dagName, nodeName, e);
                            Duration totalDuration = Duration.between(startTime, Instant.now());
                            // shouldExecute 失败，没有逻辑执行时间
                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, Duration.ZERO, e, nodeDefinition));

                            // 创建失败结果，需要 EventType
                            return Mono.just(NodeResult.failure(context,
                                    new IllegalStateException("shouldExecute failed for node " + nodeName, e),
                                    eventType)); // 使用获取到的 EventType
                        }

                        if (shouldExec) {
                            // 条件满足，执行节点逻辑 (包含重试和超时)
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 条件满足，将执行。", requestId, dagName, nodeName);
                            // 调用内部执行方法，传递类型参数 T
                            Mono<NodeResult<C, ?>> resultMono = executeNodeLogicInternal(
                                    nodeDefinition, context, accessor, timeout, retrySpec, requestId, dagName, startTime);
                            return resultMono;
                        } else {
                            // 条件不满足，跳过执行
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 条件不满足，将被跳过。", requestId, dagName, nodeName);
                            safeNotifyListeners(l -> l.onNodeSkipped(requestId, dagName, nodeName, nodeDefinition));
                            // 创建跳过结果，需要 EventType
                            NodeResult<C, ?> skippedResult = NodeResult.skipped(context, eventType);
                            return Mono.just(skippedResult);
                        }
                    })
                    // 添加顶层错误处理，捕获依赖解析失败等情况
                    .onErrorResume(error -> {
                        log.error("[RequestId: {}] DAG '{}': 节点 '{}' 在执行前或依赖解析中失败: {}", requestId, dagName, nodeName, error.getMessage(), error);
                        Duration totalDuration = Duration.between(startTime, Instant.now());
                        // 依赖解析失败，没有逻辑执行时间
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, Duration.ZERO, error, nodeDefinition));

                        // 创建失败结果，需要 EventType
                        return Mono.just(NodeResult.failure(context, error, eventType));
                    });
        });
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
        // 如果节点定义提供了有效超时，则使用它，否则使用执行器的默认值
        if (nodeTimeout != null && !nodeTimeout.isZero() && !nodeTimeout.isNegative()) {
            return nodeTimeout;
        }
        return defaultNodeTimeout; // 使用 Executor 的默认超时
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
        final List<String> dependencyNames = nodeDefinition.getDependencyNames();

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
        // collectMap 会等待所有内部 Mono<Map.Entry> 完成后，将结果收集到 Map 中
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
        // 这个 Mono 完成时，表示依赖节点的逻辑单元已完成
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
            DependencyAccessor<C> accessor, // 替换 Map
            Duration timeout,
            Retry retrySpec,
            String requestId,
            String dagName,
            Instant startTime) { // startTime 是 onNodeStart 的时间

        String nodeName = nodeDefinition.getNodeName();
        NodeLogic<C, T> nodeLogic = nodeDefinition.getNodeLogic(); // 获取具体逻辑实现
        Class<T> eventType = nodeDefinition.getEventType(); // 获取事件类型

        // 使用 deferContextual 允许在操作链中传递和访问上下文信息（例如计时）
        return Mono.deferContextual(contextView -> {
                    Instant logicStartTime = Instant.now(); // 记录逻辑执行开始时间
                    log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (逻辑: {}) 核心逻辑开始执行...",
                            requestId, dagName, nodeName, nodeLogic.getLogicIdentifier());

                    // 调用 NodeLogic 的 execute 方法
                    return nodeLogic.execute(context, accessor) // 返回 Mono<NodeResult<C, T>>
                            .subscribeOn(nodeExecutionScheduler) // 在指定调度器上执行
                            .timeout(timeout) // 应用超时
                            .doOnEach(signal -> { // 监控每个信号 (onNext, onError, onComplete)
                                // 尝试从 Reactor Context 获取逻辑开始时间
                                signal.getContextView().<Instant>getOrEmpty("logicStartTime")
                                        .ifPresent(lStartTime -> {
                                            Instant endTime = Instant.now();
                                            Duration totalDuration = Duration.between(startTime, endTime); // 总时长
                                            Duration logicDuration = Duration.between(lStartTime, endTime); // 逻辑时长

                                            if (signal.isOnNext()) { // 收到 NodeResult
                                                NodeResult<C, ?> result = (NodeResult<C, ?>) signal.get(); // 类型是 ?
                                                logResult(result, nodeName, dagName, requestId); // 记录结果日志
                                                // 通知监听器成功或失败 (基于 NodeResult 状态)
                                                if (result.isSuccess()) {
                                                    safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagName, nodeName, totalDuration, logicDuration, result, nodeDefinition));
                                                } else { // 可能是 failure 或 skipped (理论上 execute 不应返回 skipped)
                                                    Throwable err = result.getError().orElse(new RuntimeException("NodeResult in onNext indicated failure/skipped with no error object"));
                                                    log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 在 doOnEach 的 onNext 中收到非成功结果，按失败处理。", requestId, dagName, nodeName);
                                                    safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, err, nodeDefinition));
                                                }
                                            } else if (signal.isOnError()) { // 执行/超时/重试过程中发生错误
                                                Throwable error = signal.getThrowable();
                                                if (error instanceof TimeoutException) {
                                                    log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试超时 ({}).", requestId, dagName, nodeName, timeout);
                                                    // 超时也通知 onNodeTimeout
                                                    safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagName, nodeName, timeout, nodeDefinition));
                                                    // 超时最终也会导致 onNodeFailure (在 onErrorResume 中处理)
                                                } else {
                                                    log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试失败: {}", requestId, dagName, nodeName, error.getMessage());
                                                    // 其他错误，最终也会在 onErrorResume 中触发 onNodeFailure
                                                }
                                            } else if (signal.isOnComplete()) {
                                                // Mono<NodeResult> 正常完成 (即 onNext 已发出)
                                                log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 的 Mono<NodeResult> 执行流完成。", requestId, dagName, nodeName);
                                            }
                                        });
                            })
                            .retryWhen(retrySpec != null ? retrySpec : Retry.max(0)) // 应用重试策略
                            // 最终错误处理 (包括重试耗尽或非重试错误)
                            .onErrorResume(error -> Mono.deferContextual(errorContextView -> { // 使用 deferContextual 获取计时信息
                                Instant endTime = Instant.now();
                                // 尝试获取逻辑开始时间，如果失败则用节点开始时间近似
                                Instant lStartTime = errorContextView.<Instant>getOrDefault("logicStartTime", startTime);
                                Duration totalDuration = Duration.between(startTime, endTime);
                                Duration logicDuration = Duration.between(lStartTime, endTime);

                                log.error("[RequestId: {}] DAG '{}': 节点 '{}' 执行最终失败: {}",
                                        requestId, dagName, nodeName, error.getMessage(), error);

                                // 通知监听器最终失败
                                safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, error, nodeDefinition));

                                // 返回一个表示失败的 NodeResult
                                return Mono.just(NodeResult.failure(context, error, eventType)); // 使用节点定义的 EventType
                            }))
                            // 将 logicStartTime 放入 Reactor Context
                            .contextWrite(ctx -> ctx.put("logicStartTime", logicStartTime));
                })
                // 确保返回类型是 Mono<NodeResult<C, ?>>
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
            boolean hasEvents = result.getEvents() != (Flux<?>)Flux.empty(); // 检查事件流是否为空
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (事件类型: {}) 执行成功. HasEvents: {}",
                    requestId, dagName, nodeName, result.getEventType().getSimpleName(),
                    hasEvents ? "Yes" : "No");
        } else if (result.isFailure()) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' (事件类型: {}) 执行完成但返回失败: {}",
                    requestId, dagName, nodeName, result.getEventType().getSimpleName(),
                    result.getError().map(Throwable::getMessage).orElse("未知错误"));
        } else { // Skipped
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (事件类型: {}) 返回了 SKIPPED 状态 (理论上不应由 execute 产生)",
                    requestId, dagName, nodeName, result.getEventType().getSimpleName());
        }
    }

    /**
     * 安全地通知所有注册的监听器，捕获单个监听器的异常。
     *
     * @param notification 要对每个监听器执行的操作。
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
                // 继续通知其他监听器
            }
        }
    }
}
