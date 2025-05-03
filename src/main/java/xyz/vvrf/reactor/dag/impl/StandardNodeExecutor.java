// file: impl/StandardNodeExecutor.java
package xyz.vvrf.reactor.dag.impl;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener; // 保持监听器

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * 标准节点执行器 - 负责执行单个节点实例。
 * 使用 InputAccessor 处理输入，与 DagDefinition 交互获取节点实现和连接。
 *
 * @author ruifeng.wen (Refactored)
 */
@Slf4j
public class StandardNodeExecutor {

    private final Duration defaultNodeTimeout;
    private final Scheduler nodeExecutionScheduler;
    private final List<DagMonitorListener> monitorListeners;

    // 构造函数保持不变，但日志和内部逻辑会调整
    public StandardNodeExecutor(Duration defaultNodeTimeout,
                                Scheduler nodeExecutionScheduler,
                                List<DagMonitorListener> monitorListeners) {
        this.defaultNodeTimeout = Objects.requireNonNull(defaultNodeTimeout, "默认节点超时时间不能为空");
        this.nodeExecutionScheduler = Objects.requireNonNull(nodeExecutionScheduler, "节点执行调度器不能为空");
        this.monitorListeners = Objects.requireNonNull(monitorListeners, "Monitor listeners list cannot be null");
        // ... [日志] ...
        log.info("StandardNodeExecutor initialized. Default timeout: {}, Scheduler: {}, Listeners: {}",
                defaultNodeTimeout, nodeExecutionScheduler.getClass().getSimpleName(), monitorListeners.size());
    }

    // 其他构造函数保持不变
    public StandardNodeExecutor(Duration defaultNodeTimeout) {
        this(defaultNodeTimeout, Schedulers.boundedElastic(), Collections.emptyList());
    }

    @Deprecated
    public StandardNodeExecutor(Duration defaultNodeTimeout, Scheduler nodeExecutionScheduler) {
        this(defaultNodeTimeout, nodeExecutionScheduler, Collections.emptyList());
    }


    /**
     * 获取或创建节点实例执行的 Mono<NodeResult>，支持请求级缓存。
     *
     * @param <C>           上下文类型
     * @param instanceName  节点实例的名称 (来自 DagDefinition)
     * @param context       当前上下文
     * @param cache         请求级缓存 (Key: instanceName, Value: Mono<NodeResult<C, ?>>)
     * @param dagDefinition DAG 定义
     * @param requestId     请求 ID，用于日志
     * @return 返回一个 Mono，该 Mono 在订阅时会执行节点或从缓存返回结果。
     */
    public <C> Mono<NodeResult<C, ?>> getNodeExecutionMono(
            final String instanceName,
            final C context,
            final Cache<String, Mono<NodeResult<C, ?>>> cache, // Key 现在是 instanceName
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String cacheKey = instanceName; // 缓存键简化为实例名
        final String dagName = dagDefinition.getDagName();

        return Mono.defer(() -> {
            // 尝试从缓存获取 Mono<NodeResult<C, ?>>
            Mono<NodeResult<C, ?>> cachedMono = cache.getIfPresent(cacheKey);

            if (cachedMono != null) {
                log.trace("[RequestId: {}] DAG '{}': Cache hit for node instance '{}'",
                        requestId, dagName, instanceName);
                return cachedMono; // 直接返回缓存的 Mono
            } else {
                log.debug("[RequestId: {}] DAG '{}': Cache miss, creating execution Mono for node instance '{}'",
                        requestId, dagName, instanceName);

                // 创建新的执行 Mono
                Mono<NodeResult<C, ?>> newMono = createNodeExecutionMono(
                        instanceName, context, cache, dagDefinition, requestId);

                // 使用 .cache() 缓存结果，并放入 Caffeine 缓存
                Mono<NodeResult<C, ?>> monoToCache = newMono.cache();
                cache.put(cacheKey, monoToCache);

                return monoToCache;
            }
        });
    }

    /**
     * 创建节点实例执行的 Mono。
     * 这个 Mono 在订阅时会查找节点实现、准备输入、执行节点逻辑。
     */
    private <C> Mono<NodeResult<C, ?>> createNodeExecutionMono(
            final String instanceName,
            final C context,
            final Cache<String, Mono<NodeResult<C, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();

        return Mono.defer(() -> {
            // 1. 查找节点实现和输出类型
            DagNode<C, ?> nodeImplementation;
            Class<?> outputPayloadType; // 需要知道输出类型以创建失败/跳过结果
            try {
                nodeImplementation = findNodeImplementation(instanceName, dagDefinition, requestId);
                outputPayloadType = nodeImplementation.getPayloadType(); // 获取实现声明的类型
            } catch (Exception e) {
                log.error("[RequestId: {}] DAG '{}': Failed to find implementation for node instance '{}' before execution.", requestId, dagName, instanceName, e);
                // 无法获取 Payload 类型，使用 Object.class 作为后备？或者直接错误？
                // 最好是能从 DagDefinition 获取，即使实现查找失败
                Class<?> fallbackType = dagDefinition.getNodeOutputType(instanceName).orElse(Object.class);
                // 注意：这里无法通知监听器，因为没有 node 对象
                return Mono.just(NodeResult.failure(context, e, fallbackType)); // 返回失败结果
            }

            Duration timeout = determineNodeTimeout(nodeImplementation);
            Instant startTime = Instant.now();
            // 使用 instanceName 通知监听器
            safeNotifyListeners(l -> l.onNodeStart(requestId, dagName, instanceName, nodeImplementation));

            log.debug("[RequestId: {}] DAG '{}': Preparing execution for node '{}' (Impl: {}, Timeout: {})",
                    requestId, dagName, instanceName, nodeImplementation.getClass().getSimpleName(), timeout);

            // 2. 准备输入 (递归获取上游结果)
            Mono<Map<String, NodeResult<C, ?>>> upstreamResultsMono = prepareInputs(
                    instanceName, context, cache, dagDefinition, requestId);

            // 3. 执行或跳过
            return upstreamResultsMono
                    .flatMap(upstreamResults -> {
                        InputAccessor<C> inputAccessor = new DefaultInputAccessor<>(instanceName, dagDefinition, upstreamResults);

                        boolean shouldExec;
                        try {
                            shouldExec = nodeImplementation.shouldExecute(context, inputAccessor);
                        } catch (Exception e) {
                            log.error("[RequestId: {}] DAG '{}': Node '{}' shouldExecute method threw exception. Treating as skipped/failed.",
                                    requestId, dagName, instanceName, e);
                            Duration totalDuration = Duration.between(startTime, Instant.now());
                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, totalDuration, Duration.ZERO, e, nodeImplementation));
                            // 返回失败结果，使用已知的 outputPayloadType
                            return Mono.just(NodeResult.failure(context,
                                    new IllegalStateException("shouldExecute failed for node " + instanceName, e),
                                    outputPayloadType));
                        }

                        if (shouldExec) {
                            log.debug("[RequestId: {}] DAG '{}': Node '{}' condition met, proceeding to execute.", requestId, dagName, instanceName);
                            // 执行节点逻辑 (泛型 P 在这里丢失，需要类型转换)
                            Mono<NodeResult<C, ?>> resultMono = executeNodeInternal(
                                    nodeImplementation, context, inputAccessor, timeout, requestId, dagName, instanceName, startTime);
                            return resultMono;
                        } else {
                            log.debug("[RequestId: {}] DAG '{}': Node '{}' condition not met, skipping execution.", requestId, dagName, instanceName);
                            safeNotifyListeners(l -> l.onNodeSkipped(requestId, dagName, instanceName, nodeImplementation));
                            // 返回跳过结果，使用已知的 outputPayloadType
                            return Mono.just(NodeResult.skipped(context, outputPayloadType));
                        }
                    })
                    // 顶层错误处理 (例如输入准备失败)
                    .onErrorResume(error -> {
                        log.error("[RequestId: {}] DAG '{}': Node '{}' failed during input preparation or pre-execution: {}", requestId, dagName, instanceName, error.getMessage(), error);
                        Duration totalDuration = Duration.between(startTime, Instant.now());
                        // 使用 instanceName 和已知的 nodeImplementation
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, totalDuration, Duration.ZERO, error, nodeImplementation));
                        // 返回失败结果，使用已知的 outputPayloadType
                        return Mono.just(NodeResult.failure(context, error, outputPayloadType));
                    });
        });
    }

    /**
     * 查找节点实例的实现。
     */
    private <C> DagNode<C, ?> findNodeImplementation(
            String instanceName,
            DagDefinition<C> dagDefinition,
            String requestId) {
        return dagDefinition.getNodeImplementation(instanceName)
                .orElseThrow(() -> {
                    String dagName = dagDefinition.getDagName();
                    String errorMsg = String.format("Node instance '%s' not found in DAG '%s'.", instanceName, dagName);
                    log.error("[RequestId: {}] {}", requestId, errorMsg);
                    return new IllegalStateException(errorMsg);
                });
    }

    /**
     * 确定节点的实际执行超时时间。
     */
    private <C> Duration determineNodeTimeout(DagNode<C, ?> node) {
        Duration nodeTimeout = node.getExecutionTimeout();
        if (nodeTimeout != null && !nodeTimeout.isZero() && !nodeTimeout.isNegative()) {
            return nodeTimeout;
        }
        return defaultNodeTimeout;
    }

    /**
     * 准备节点的所有输入。
     * 返回一个 Mono，该 Mono 完成时会发出一个 Map，包含所有直接上游节点的实例名称及其对应的 NodeResult。
     */
    private <C> Mono<Map<String, NodeResult<C, ?>>> prepareInputs(
            final String instanceName,
            final C context,
            final Cache<String, Mono<NodeResult<C, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();
        Map<String, String> wiring = dagDefinition.getUpstreamWiring(instanceName); // inputSlot -> upstreamInstance

        if (wiring.isEmpty()) {
            return Mono.just(Collections.emptyMap());
        }

        Set<String> upstreamNodeNames = new HashSet<>(wiring.values()); // 获取所有唯一的上游节点名

        log.debug("[RequestId: {}] DAG '{}': Node '{}' preparing inputs from upstream nodes: {}",
                requestId, dagName, instanceName, upstreamNodeNames);

        // 为每个上游节点创建一个获取其 NodeResult 的 Mono
//        List<Mono<Map.Entry<String, NodeResult<C, ?>>>> upstreamResultMonos = upstreamNodeNames.stream()
//                .map(upstreamName ->
//                        getNodeExecutionMono(upstreamName, context, cache, dagDefinition, requestId) // 递归调用
//                                .map(result -> new AbstractMap.SimpleEntry<>(upstreamName, result)) // 转为 Map.Entry
//                                .onErrorResume(e -> { // 处理单个上游获取失败的情况
//                                    log.error("[RequestId: {}] DAG '{}': Failed to get result for upstream node '{}' needed by '{}': {}",
//                                            requestId, dagName, upstreamName, instanceName, e.getMessage());
//                                    // 根据错误处理策略决定是否继续？目前 FAIL_FAST 会在引擎层处理。
//                                    // 这里可以选择返回一个包含错误的 Mono.error，或者一个代表失败的 Entry？
//                                    // 返回 Mono.error 更符合响应式风格
//                                    return Mono.error(new RuntimeException(String.format("Failed to resolve dependency '%s' for node '%s'", upstreamName, instanceName), e));
//                                })
//                )
//                .collect(Collectors.toList());

        // 并发执行所有上游 Mono，收集结果到 Map
        return Flux.fromIterable(upstreamNodeNames) // Flux<String>
                .flatMap(upstreamName -> // Function<String, Publisher<Map.Entry<String, NodeResult<C, ?>>>>
                        getNodeExecutionMono(upstreamName, context, cache, dagDefinition, requestId) // Mono<NodeResult<C, ?>>
                                .map(result -> (Map.Entry<String, NodeResult<C, ?>>) new AbstractMap.SimpleEntry<>(upstreamName, result)) // Map to Entry inside Mono
                                .onErrorResume(e -> { // Handle error during *getting* the upstream result
                                    log.error("[RequestId: {}] DAG '{}': Failed to get result for upstream node '{}' needed by '{}': {}",
                                            requestId, dagName, upstreamName, instanceName, e.getMessage());
                                    // Propagate the error to fail the input preparation
                                    return Mono.error(new RuntimeException(String.format("Failed to resolve dependency '%s' for node '%s'", upstreamName, instanceName), e));
                                })
                ) // Result of flatMap is Flux<Map.Entry<String, NodeResult<C, ?>>>
                .collectMap(Map.Entry::getKey, Map.Entry::getValue) // Collect into Mono<Map<String, NodeResult<C, ?>>>
                // *** 修改结束 ***
                .doOnSuccess(results -> log.debug(
                        "[RequestId: {}] DAG '{}': Node '{}' successfully prepared inputs from {} upstream nodes.",
                        requestId, dagName, instanceName, results.size()))
                .doOnError(e -> log.error(
                        "[RequestId: {}] DAG '{}': Node '{}' failed during input preparation: {}",
                        requestId, dagName, instanceName, e.getMessage(), e)); // Log final error
    }


    /**
     * 内部方法：实际执行节点逻辑。
     * 返回 Mono<NodeResult<C, ?>>，因为 P 类型在调用栈中丢失了。
     */
    @SuppressWarnings({"unchecked", "rawtypes"}) // 需要处理泛型丢失
    private <C> Mono<NodeResult<C, ?>> executeNodeInternal(
            DagNode<C, ?> node, // 传入的是 DagNode<C, ?>
            C context,
            InputAccessor<C> inputAccessor,
            Duration timeout,
            String requestId,
            String dagName,
            String instanceName, // 传入实例名用于日志和监听器
            Instant startTime) {

        Class<?> expectedPayloadType = node.getPayloadType(); // 获取期望的输出类型
        Retry retrySpec = node.getRetrySpec();

        // 需要强制转换 node 到正确的类型 DagNode<C, P> 才能调用 execute
        // 这是泛型擦除带来的问题，或者需要重新设计 execute 调用方式
        DagNode rawNode = node; // 避免编译警告

        return Mono.deferContextual(contextView -> {
            Instant logicStartTime = Instant.now();
            log.debug("[RequestId: {}] DAG '{}': Node '{}' (Payload: {}, Impl: {}) core logic execution starting...",
                    requestId, dagName, instanceName, expectedPayloadType.getSimpleName(), node.getClass().getSimpleName());

            // 调用 execute，返回 Mono<NodeResult<C, P>>，但 P 未知
            Mono<NodeResult<C, ?>> executionMono = rawNode.execute(context, inputAccessor);

            return executionMono
                    .subscribeOn(nodeExecutionScheduler)
                    .timeout(timeout)
                    .doOnEach(signal -> { // 日志和监听器通知
                        signal.getContextView().<Instant>getOrEmpty("logicStartTime")
                                .ifPresent(lStartTime -> {
                                    Instant endTime = Instant.now();
                                    Duration totalDuration = Duration.between(startTime, endTime);
                                    Duration logicDuration = Duration.between(lStartTime, endTime);

                                    if (signal.isOnNext()) {
                                        NodeResult<C, ?> result = (NodeResult<C, ?>) signal.get();
                                        validateAndLogResult(result, expectedPayloadType, instanceName, dagName, requestId);
                                        if (result.isSuccess()) {
                                            safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagName, instanceName, totalDuration, logicDuration, result, node));
                                        } else { // 理论上 execute 不应返回非成功的 NodeResult，而应是 Mono.error
                                            Throwable err = result.getError().orElse(new RuntimeException("NodeResult in onNext indicated failure with no error object"));
                                            log.warn("[RequestId: {}] DAG '{}': Node '{}' execute() returned a non-SUCCESS NodeResult. Treating as failure.", requestId, dagName, instanceName);
                                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, totalDuration, logicDuration, err, node));
                                        }
                                    } else if (signal.isOnError()) {
                                        Throwable error = signal.getThrowable();
                                        if (error instanceof TimeoutException) {
                                            log.warn("[RequestId: {}] DAG '{}': Node '{}' execution attempt timed out after {}.", requestId, dagName, instanceName, timeout);
                                            safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagName, instanceName, timeout, node));
                                        } else {
                                            log.warn("[RequestId: {}] DAG '{}': Node '{}' execution attempt failed: {}", requestId, dagName, instanceName, error.getMessage());
                                            // 失败通知在下面的 onErrorResume 中处理
                                        }
                                    }
                                });
                    })
                    .retryWhen(retrySpec != null ? retrySpec : Retry.max(0)) // 应用重试
                    .onErrorResume(error -> Mono.deferContextual(errorContextView -> { // 最终错误处理
                        Instant endTime = Instant.now();
                        Instant lStartTime = errorContextView.<Instant>getOrDefault("logicStartTime", startTime);
                        Duration totalDuration = Duration.between(startTime, endTime);
                        Duration logicDuration = Duration.between(lStartTime, endTime);

                        log.error("[RequestId: {}] DAG '{}': Node '{}' execution ultimately failed: {}",
                                requestId, dagName, instanceName, error.getMessage(), error);

                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, totalDuration, logicDuration, error, node));
                        // 返回失败的 NodeResult，使用期望的 Payload 类型
                        return Mono.just(NodeResult.failure(context, error, expectedPayloadType));
                    }))
                    .contextWrite(ctx -> ctx.put("logicStartTime", logicStartTime)); // 传递逻辑开始时间
        });
    }

    /**
     * 验证节点返回的 NodeResult 并记录日志。
     */
    private <C> void validateAndLogResult(
            NodeResult<C, ?> result, // 结果类型是 ?
            Class<?> expectedPayloadType, // 期望的类型
            String instanceName,
            String dagName,
            String requestId) {

        Class<?> actualPayloadType = result.getPayloadType();

        if (actualPayloadType == null) {
            log.warn("[RequestId: {}] DAG '{}': Node '{}' returned NodeResult with null payload type!",
                    requestId, dagName, instanceName);
        } else if (!expectedPayloadType.equals(actualPayloadType)) {
            // 允许子类兼容
            if (!expectedPayloadType.isAssignableFrom(actualPayloadType)) {
                log.error("[RequestId: {}] DAG '{}': Node '{}' returned NodeResult payload type ({}) is incompatible with expected type ({})!",
                        requestId, dagName, instanceName,
                        actualPayloadType.getSimpleName(),
                        expectedPayloadType.getSimpleName());
            } else {
                log.trace("[RequestId: {}] DAG '{}': Node '{}' returned payload type ({}) is assignable to expected type ({}).",
                        requestId, dagName, instanceName,
                        actualPayloadType.getSimpleName(),
                        expectedPayloadType.getSimpleName());
            }
        }

        if (result.isSuccess()) {
            log.debug("[RequestId: {}] DAG '{}': Node '{}' (Expected Payload: {}) executed successfully. Result: {}",
                    requestId, dagName, instanceName, expectedPayloadType.getSimpleName(), result);
        } else {
            // 这个分支理论上不应该在 onNext 中出现
            log.warn("[RequestId: {}] DAG '{}': Node '{}' (Expected Payload: {}) completed with non-SUCCESS status in onNext: {}",
                    requestId, dagName, instanceName, expectedPayloadType.getSimpleName(), result);
        }
    }

    /**
     * 安全地通知所有注册的监听器。 (保持不变)
     */
    private void safeNotifyListeners(java.util.function.Consumer<DagMonitorListener> notification) {
        if (monitorListeners.isEmpty()) {
            return;
        }
        for (DagMonitorListener listener : monitorListeners) {
            try {
                notification.accept(listener);
            } catch (Exception e) {
                log.error("DAG Monitor Listener {} threw exception: {}", listener.getClass().getName(), e.getMessage(), e);
            }
        }
    }
}
