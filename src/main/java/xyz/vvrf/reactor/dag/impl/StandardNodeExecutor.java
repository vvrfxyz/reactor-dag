// [Modified File]: standardnodeexecutor.java
package xyz.vvrf.reactor.dag.impl;

// 引入 InputDependencyAccessor 和 InputRequirement
import xyz.vvrf.reactor.dag.core.InputDependencyAccessor;
import xyz.vvrf.reactor.dag.core.InputRequirement;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.core.*; // 引入 core 包
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;

import java.time.Instant;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * 标准节点执行器 - 负责执行单个节点并处理依赖关系和缓存。
 * 使用 DagDefinition 获取执行前驱和输入映射。
 *
 * @author ruifeng.wen
 */
@Slf4j
public class StandardNodeExecutor {

    private final Duration defaultNodeTimeout;
    private final Scheduler nodeExecutionScheduler;
    private final List<DagMonitorListener> monitorListeners;

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
    public StandardNodeExecutor(Duration defaultNodeTimeout) {
        this(defaultNodeTimeout, Schedulers.boundedElastic(), Collections.emptyList());
    }
    @Deprecated
    public StandardNodeExecutor(Duration defaultNodeTimeout, Scheduler nodeExecutionScheduler) {
        this(defaultNodeTimeout, nodeExecutionScheduler, Collections.emptyList());
    }


    /**
     * 获取或创建节点执行的 Mono<NodeResult>，支持请求级缓存。
     *
     * @param <C>           上下文类型
     * @param <P>           期望的节点 Payload 类型 (用于查找节点，结果类型由节点决定)
     * @param nodeName      节点名称
     * @param payloadType   期望的节点 Payload 类型 Class 对象 (用于查找节点)
     * @param context       当前上下文
     * @param cache         请求级缓存 (Key: String, Value: Mono<? extends NodeResult<C, ?, ?>>)
     * @param dagDefinition DAG 定义
     * @param requestId     请求 ID，用于日志
     * @return 返回一个 Mono，该 Mono 在订阅时会执行节点或从缓存返回结果。
     * 结果 NodeResult 的 Payload 类型为 P，Event Data 类型为通配符 ?。
     */
    public <C, P> Mono<NodeResult<C, P, ?>> getNodeExecutionMono(
            final String nodeName,
            final Class<P> payloadType, // payloadType 仍然用于查找节点
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String cacheKey = nodeName; // 使用 nodeName 作为缓存 Key
        final String dagName = dagDefinition.getDagName();

        return Mono.defer(() -> {
            Mono<? extends NodeResult<C, ?, ?>> cachedMono = cache.getIfPresent(cacheKey);

            if (cachedMono != null) {
                log.trace("[RequestId: {}] DAG '{}': 缓存命中节点 '{}'",
                        requestId, dagName, nodeName);
                return castResultMono(cachedMono, payloadType, nodeName, dagName, requestId);
            } else {
                log.debug("[RequestId: {}] DAG '{}': 缓存未命中，创建节点 '{}' 执行 Mono",
                        requestId, dagName, nodeName);

                // 创建 Mono 时不再传入 payloadType，因为节点实例已经确定了输出类型
                Mono<NodeResult<C, ?, ?>> newMono = createNodeExecutionMonoInternal(
                        nodeName, context, cache, dagDefinition, requestId);

                // 使用 .cache() 缓存结果
                Mono<NodeResult<C, ?, ?>> monoToCache = newMono.cache();
                cache.put(cacheKey, monoToCache);

                // 返回缓存的 Mono，并进行类型转换/检查
                return castResultMono(monoToCache, payloadType, nodeName, dagName, requestId);
            }
        });
    }

    /**
     * 辅助方法：将缓存或新创建的 Mono<NodeResult<C, ?, ?>> 转换为调用者期望的 Mono<NodeResult<C, P, ?>>
     * 同时进行类型检查。
     */
    @SuppressWarnings("unchecked")
    private <C, P> Mono<NodeResult<C, P, ?>> castResultMono(
            Mono<? extends NodeResult<C, ?, ?>> sourceMono,
            Class<P> expectedPayloadType,
            String nodeName, String dagName, String requestId) {

        return sourceMono.flatMap(result -> {
            // 检查实际结果的 Payload 类型是否兼容期望的类型 P
            if (expectedPayloadType.isAssignableFrom(result.getPayloadType())) {
                // 类型兼容，安全转换
                return Mono.just((NodeResult<C, P, ?>) result);
            } else {
                // 类型不兼容，这是一个错误情况
                String errorMsg = String.format("[%s] DAG '%s': Node '%s' executed, but its result payload type (%s) is not assignable to the requested type (%s).",
                        requestId, dagName, nodeName, result.getPayloadType().getSimpleName(), expectedPayloadType.getSimpleName());
                log.error(errorMsg);
                // 返回一个表示错误的 Mono，或者一个包含错误的 NodeResult?
                // 返回错误 Mono 更符合 Reactor 风格
                return Mono.error(new ClassCastException(errorMsg));
            }
        });
    }


    /**
     * 创建节点执行的 Mono (内部版本，不关心请求的 P 类型)。
     * 这个 Mono 在订阅时会查找节点、解析依赖、执行节点逻辑。
     * 返回 Mono<NodeResult<C, ?, ?>>
     */
    private <C> Mono<NodeResult<C, ?, ?>> createNodeExecutionMonoInternal(
            final String nodeName,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();

        return Mono.defer(() -> {
            // 1. 获取节点实例 (不关心具体类型)
            DagNode<C, ?, ?> node;
            try {
                // 使用 getNodeAnyType 获取节点实例
                node = dagDefinition.getNodeAnyType(nodeName)
                        .orElseThrow(() -> new IllegalStateException(
                                String.format("Node '%s' not found in DAG '%s' during execution preparation.", nodeName, dagName)));
            } catch (Exception e) {
                log.error("[RequestId: {}] DAG '{}': 执行前查找节点 '{}' 失败。", requestId, dagName, nodeName, e);
                return Mono.error(e);
            }

            // 2. 确定超时
            Duration timeout = determineNodeTimeout(node);

            Instant startTime = Instant.now();
            safeNotifyListeners(l -> l.onNodeStart(requestId, dagName, nodeName, node));

            log.debug("[RequestId: {}] DAG '{}': 准备执行节点 '{}' (Impl: {}), 超时: {}",
                    requestId, dagName, nodeName, node.getClass().getSimpleName(), timeout);

            // 3. 解析执行前驱节点的 Mono<NodeResult>
            Mono<Map<String, NodeResult<C, ?, ?>>> predecessorsMono = resolvePredecessors(
                    nodeName, context, cache, dagDefinition, requestId);

            // 4. 执行
            return predecessorsMono
                    .flatMap(predecessorResults -> {
                        // 5. 创建 InputDependencyAccessor
                        // 获取当前节点的输入映射 (已在 Definition 初始化时验证和解析)
                        Map<InputRequirement<?>, String> nodeInputMappings = dagDefinition.getInputMappingForNode(nodeName);
                        InputDependencyAccessor<C> accessor = new DefaultInputDependencyAccessor<>(
                                predecessorResults, nodeInputMappings, nodeName, dagName);

                        // 6. 检查 shouldExecute
                        boolean shouldExec;
                        try {
                            // 传递新的 Accessor
                            shouldExec = node.shouldExecute(context, accessor);
                        } catch (Exception e) {
                            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 的 shouldExecute 方法抛出异常，将视为不执行并产生错误结果。",
                                    requestId, dagName, nodeName, e);
                            Duration totalDuration = Duration.between(startTime, Instant.now());
                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, Duration.ZERO, e, node));
                            // 需要知道节点的 P 和 T 类型来创建 failure result
                            return Mono.just(createFailureResult(context, e, node)); // 辅助方法创建结果
                        }

                        // 7. 执行或跳过
                        if (shouldExec) {
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 条件满足，将执行。", requestId, dagName, nodeName);
                            // 调用 executeNodeInternal，传递新的 Accessor
                            Mono<? extends NodeResult<C, ?, ?>> resultMono = executeNodeLogic(
                                    node, context, accessor, timeout, requestId, dagName, startTime);
                            return resultMono;
                        } else {
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 条件不满足，将被跳过。", requestId, dagName, nodeName);
                            safeNotifyListeners(l -> l.onNodeSkipped(requestId, dagName, nodeName, node));
                            // 需要知道节点的 P 和 T 类型来创建 skipped result
                            NodeResult<C, ?, ?> skippedResult = createSkippedResult(context, node); // 辅助方法
                            return Mono.just(skippedResult);
                        }
                    })
                    // 顶层错误处理 (例如前驱解析失败)
                    .onErrorResume(error -> {
                        log.error("[RequestId: {}] DAG '{}': 节点 '{}' 在执行前或前驱解析中失败: {}", requestId, dagName, nodeName, error.getMessage(), error);
                        Duration totalDuration = Duration.between(startTime, Instant.now());
                        DagNode<C, ?, ?> finalNode = node; // 捕获 node 实例
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, Duration.ZERO, error, finalNode));
                        return Mono.just(createFailureResult(context, error, node)); // 使用辅助方法
                    });
        });
    }

    /**
     * 解析节点的所有直接执行前驱。
     * 返回一个 Mono，该 Mono 完成时会发出一个 Map，包含所有前驱节点的名称及其对应的 NodeResult。
     * 利用 getNodeExecutionMono 来获取前驱节点的 Mono，从而复用缓存逻辑。
     */
    private <C> Mono<Map<String, NodeResult<C, ?, ?>>> resolvePredecessors(
            final String nodeName,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();
        Set<String> predecessorNames = dagDefinition.getExecutionPredecessors(nodeName);

        if (predecessorNames.isEmpty()) {
            return Mono.just(Collections.emptyMap());
        }

        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 正在解析 {} 个执行前驱: {}",
                requestId, dagName, nodeName, predecessorNames.size(), predecessorNames);

        // 列表类型声明保持不变
        List<Mono<? extends Map.Entry<String, NodeResult<C, ?, ?>>>> predecessorMonos = predecessorNames.stream()
                .map(predName -> {
                    // 直接调用 getNodeExecutionMono 获取前驱节点的 Mono
                    Mono<NodeResult<C, Object, ?>> predecessorResultMonoTyped = getNodeExecutionMono(
                            predName,
                            Object.class, // 使用 Object.class
                            context,
                            cache,
                            dagDefinition,
                            requestId
                    );

                    // 将 Mono<NodeResult<C, Object, ?>> 转换为 Mono<? extends NodeResult<C, ?, ?>>
                    Mono<? extends NodeResult<C, ?, ?>> predecessorResultMonoWildcard = predecessorResultMonoTyped;

                    // 当 Mono 完成时，创建 Map.Entry
                    // 创建 Mono<AbstractMap.SimpleEntry<...>>
                    Mono<AbstractMap.SimpleEntry<String, NodeResult<C, ?, ?>>> monoSimpleEntry =
                            predecessorResultMonoWildcard.map(result ->
                                    new AbstractMap.SimpleEntry<>(predName, result)
                            );

                    // *** 在 map lambda 返回时进行显式类型转换 ***
                    // 将 Mono<AbstractMap.SimpleEntry<...>> 转换为 Mono<? extends Map.Entry<...>>
                    // 这样流元素的类型就能被 collect 正确推断
                    return (Mono<? extends Map.Entry<String, NodeResult<C, ?, ?>>>) monoSimpleEntry;
                })
                .collect(Collectors.toList()); // 现在 collect 可以正确处理

        // 使用 Flux.merge 并发执行所有前驱 Mono
        return Flux.merge(predecessorMonos)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .doOnSuccess(results -> log.debug(
                        "[RequestId: {}] DAG '{}': 节点 '{}' 的所有 {} 个前驱 NodeResult 已就绪。",
                        requestId, dagName, nodeName, results.size()))
                .doOnError(e -> log.error(
                        "[RequestId: {}] DAG '{}': 节点 '{}' 获取前驱 NodeResult 失败: {}",
                        requestId, dagName, nodeName, e.getMessage(), e));
    }





    /**
     * 内部方法：实际执行节点逻辑。
     * 使用新的 InputDependencyAccessor。
     * 返回 Mono<NodeResult<C, P, T>>，其中 P, T 由节点决定。
     */
    private <C, P, T> Mono<NodeResult<C, P, T>> executeNodeLogic(
            DagNode<C, P, T> node, // 传入具体类型的节点
            C context,
            InputDependencyAccessor<C> accessor, // 传入新的 Accessor
            Duration timeout,
            String requestId,
            String dagName,
            Instant startTime) {

        String nodeName = node.getName();
        Retry retrySpec = node.getRetrySpec();

        return Mono.deferContextual(contextView -> {
            Instant logicStartTime = Instant.now();
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Payload: {}, Event: {}, Impl: {}) 核心逻辑计时开始...",
                    requestId, dagName, nodeName, node.getPayloadType().getSimpleName(), node.getEventType().getSimpleName(), node.getClass().getSimpleName());

            // 调用节点的 execute 方法，传入新的 accessor
            return node.execute(context, accessor)
                    .subscribeOn(nodeExecutionScheduler)
                    .timeout(timeout)
                    .doOnEach(signal -> { // 日志和监控逻辑基本不变，但使用 node 实例获取类型
                        signal.getContextView().<Instant>getOrEmpty("logicStartTime")
                                .ifPresent(lStartTime -> {
                                    Instant endTime = Instant.now();
                                    Duration totalDuration = Duration.between(startTime, endTime);
                                    Duration logicDuration = Duration.between(lStartTime, endTime);

                                    if (signal.isOnNext()) {
                                        @SuppressWarnings("unchecked") // 从 signal 获取时类型是 Object
                                        NodeResult<C, P, T> result = (NodeResult<C, P, T>) signal.get();
                                        // 验证结果类型 (相对于节点自身声明的类型)
                                        validateResultTypes(result, node, nodeName, dagName, requestId);
                                        if (result.isSuccess()) {
                                            safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagName, nodeName, totalDuration, logicDuration, result, node));
                                        } else {
                                            Throwable err = result.getError().orElse(new RuntimeException("NodeResult in onNext indicated failure with no error object"));
                                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 在 doOnEach 的 onNext 中收到非成功结果，按失败处理。", requestId, dagName, nodeName);
                                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, err, node));
                                        }
                                    } else if (signal.isOnError()) {
                                        Throwable error = signal.getThrowable();
                                        if (error instanceof TimeoutException) {
                                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试（在 doOnEach 中检测到）超时 ({}).", requestId, dagName, nodeName, timeout);
                                            safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagName, nodeName, timeout, node));
                                        } else {
                                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试（在 doOnEach 中检测到）失败: {}", requestId, dagName, nodeName, error.getMessage());
                                            // 失败通知在 onErrorResume 中处理
                                        }
                                    }
                                });
                    })
                    .retryWhen(retrySpec != null ? retrySpec : Retry.max(0))
                    .onErrorResume(error -> Mono.deferContextual(errorContextView -> { // 错误处理
                        Instant endTime = Instant.now();
                        Instant lStartTime = errorContextView.<Instant>getOrDefault("logicStartTime", startTime);
                        Duration totalDuration = Duration.between(startTime, endTime);
                        Duration logicDuration = Duration.between(lStartTime, endTime);

                        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 执行最终失败: {}",
                                requestId, dagName, nodeName, error.getMessage(), error);

                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, error, node));

                        // 返回包含具体 P, T 类型的 Failure NodeResult
                        return Mono.just(NodeResult.failure(context, error, node));
                    }))
                    .contextWrite(ctx -> ctx.put("logicStartTime", logicStartTime));
        });
    }

    /**
     * 验证节点返回的 NodeResult 的类型与其自身声明是否一致。
     */
    private <C, P, T> void validateResultTypes(
            NodeResult<C, P, T> result,
            DagNode<C, P, T> node, // 传入节点实例以获取声明的类型
            String nodeName,
            String dagName,
            String requestId) {

        Class<P> declaredPayloadType = node.getPayloadType();
        Class<T> declaredEventType = node.getEventType();
        Class<?> actualPayloadType = result.getPayloadType(); // NodeResult 存储的类型
        Class<?> actualEventType = result.getEventType();   // NodeResult 存储的类型

        if (actualPayloadType == null || !declaredPayloadType.equals(actualPayloadType)) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 返回的 NodeResult Payload 类型 ({}) 与其声明 ({}) 不符!",
                    requestId, dagName, nodeName,
                    actualPayloadType != null ? actualPayloadType.getSimpleName() : "null",
                    declaredPayloadType.getSimpleName());
        }
        if (actualEventType == null || !declaredEventType.equals(actualEventType)) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 返回的 NodeResult Event 类型 ({}) 与其声明 ({}) 不符!",
                    requestId, dagName, nodeName,
                    actualEventType != null ? actualEventType.getSimpleName() : "null",
                    declaredEventType.getSimpleName());
        }

        // 日志记录结果状态
        if (result.isSuccess()) {
            boolean hasEvents = result.getEvents() != Flux.<Event<T>>empty(); // 比较是否为空 Flux
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Declared P: {}, E: {}) 执行成功. Result P: {}, E: {}, Payload: {}, HasEvents: {}",
                    requestId, dagName, nodeName,
                    declaredPayloadType.getSimpleName(), declaredEventType.getSimpleName(),
                    actualPayloadType != null ? actualPayloadType.getSimpleName() : "null",
                    actualEventType != null ? actualEventType.getSimpleName() : "null",
                    result.getPayload().isPresent() ? "Present" : "Empty",
                    hasEvents ? "Yes" : "No");
        } else {
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Declared P: {}, E: {}) 执行完成，但返回了错误: {}",
                    requestId, dagName, nodeName,
                    declaredPayloadType.getSimpleName(), declaredEventType.getSimpleName(),
                    result.getError().map(Throwable::getMessage).orElse("未知错误"));
        }
    }


    // --- 辅助方法 ---
    private <C, P, T> Duration determineNodeTimeout(DagNode<C, P, T> node) {
        Duration nodeTimeout = node.getExecutionTimeout();
        return (nodeTimeout != null && !nodeTimeout.isZero() && !nodeTimeout.isNegative()) ? nodeTimeout : defaultNodeTimeout;
    }

    // 辅助方法创建 Failure NodeResult，需要知道节点的 P, T 类型
    private <C, P, T> NodeResult<C, P, T> createFailureResult(C context, Throwable error, DagNode<C, P, T> node) {
        return NodeResult.failure(context, error, node);
    }

    // 辅助方法创建 Skipped NodeResult，需要知道节点的 P, T 类型
    private <C, P, T> NodeResult<C, P, T> createSkippedResult(C context, DagNode<C, P, T> node) {
        return NodeResult.skipped(context, node);
    }

    // safeNotifyListeners 方法保持不变
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

    // --- 移除不再需要的 findNode 方法 ---
    // private <C, P> DagNode<C, P, ?> findNode(...) // 移除

    // --- 移除 resolveSingleDependency, handleDependencyResolutionError, createMapEntry ---
    // 这些逻辑被 resolvePredecessors 替代

}
