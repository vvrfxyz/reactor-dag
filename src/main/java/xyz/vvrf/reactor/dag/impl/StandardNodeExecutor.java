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
 * 缓存获取逻辑已简化。
 *
 * @author ruifeng.wen (Refactored & Modified)
 */
@Slf4j
public class StandardNodeExecutor {

    private final Duration defaultNodeTimeout;
    private final Scheduler nodeExecutionScheduler;
    private final List<DagMonitorListener> monitorListeners;

    // 构造函数保持不变
    public StandardNodeExecutor(Duration defaultNodeTimeout,
                                Scheduler nodeExecutionScheduler,
                                List<DagMonitorListener> monitorListeners) {
        this.defaultNodeTimeout = Objects.requireNonNull(defaultNodeTimeout, "默认节点超时时间不能为空");
        this.nodeExecutionScheduler = Objects.requireNonNull(nodeExecutionScheduler, "节点执行调度器不能为空");
        this.monitorListeners = Objects.requireNonNull(monitorListeners, "Monitor listeners list cannot be null");
        log.info("StandardNodeExecutor initialized. Default timeout: {}, Scheduler: {}, Listeners: {}",
                defaultNodeTimeout, nodeExecutionScheduler.getClass().getSimpleName(), monitorListeners.size());
    }

    public StandardNodeExecutor(Duration defaultNodeTimeout) {
        this(defaultNodeTimeout, Schedulers.boundedElastic(), Collections.emptyList());
    }

    @Deprecated
    public StandardNodeExecutor(Duration defaultNodeTimeout, Scheduler nodeExecutionScheduler) {
        this(defaultNodeTimeout, nodeExecutionScheduler, Collections.emptyList());
    }


    /**
     * 获取或创建节点实例执行的 Mono<NodeResult>，利用请求级缓存进行 memoization。
     * 使用 Caffeine Cache 的 compute-if-absent 模式。
     *
     * @param <C>           上下文类型
     * @param instanceName  节点实例的名称 (来自 DagDefinition)
     * @param context       当前上下文
     * @param cache         请求级缓存 (Key: instanceName, Value: Mono<NodeResult<C, ?>>) 由引擎传入
     * @param dagDefinition DAG 定义
     * @param requestId     请求 ID，用于日志
     * @return 返回一个 Mono，该 Mono 在订阅时会执行节点或从缓存返回结果。结果 Mono 本身会被 .cache() 以确保实际逻辑只执行一次。
     */
    public <C> Mono<NodeResult<C, ?>> getNodeExecutionMono(
            final String instanceName,
            final C context,
            final Cache<String, Mono<NodeResult<C, ?>>> cache, // 引擎传入的缓存
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String cacheKey = instanceName;
        final String dagName = dagDefinition.getDagName();

        // 使用 Caffeine 的 get 方法，如果 key 不存在，则执行提供的 lambda 来创建值
        // lambda 返回的 Mono 会被放入缓存，并返回给调用者
        return cache.get(cacheKey, key -> {
            log.debug("[RequestId: {}] DAG '{}': Cache miss or creating execution Mono for node instance '{}'",
                    requestId, dagName, instanceName);

            // 创建新的执行 Mono
            Mono<NodeResult<C, ?>> executionMono = createNodeExecutionMono(
                    instanceName, context, cache, dagDefinition, requestId); // 仍然需要传递 cache 用于递归准备输入

            // 使用 .cache() 确保底层节点逻辑对于此 Mono 实例只执行一次
            // 这个被 .cache() 包装后的 Mono 被存入 Caffeine 缓存
            return executionMono.cache();
        });
    }

    /**
     * 创建节点实例执行的 Mono (未缓存状态)。
     * 这个 Mono 在订阅时会查找节点实现、准备输入、执行节点逻辑。
     * 它会被调用者（getNodeExecutionMono）使用 .cache() 包装。
     */
    private <C> Mono<NodeResult<C, ?>> createNodeExecutionMono(
            final String instanceName,
            final C context,
            final Cache<String, Mono<NodeResult<C, ?>>> cache, // 用于准备输入
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();

        // 使用 Mono.defer 确保每次订阅时都执行查找和准备逻辑（除非被 .cache() 优化掉）
        return Mono.defer(() -> {
            // 1. 查找节点实现和输出类型
            DagNode<C, ?> nodeImplementation;
            Class<?> outputPayloadType;
            try {
                nodeImplementation = findNodeImplementation(instanceName, dagDefinition, requestId);
                outputPayloadType = nodeImplementation.getPayloadType();
            } catch (Exception e) {
                log.error("[RequestId: {}] DAG '{}': Failed to find implementation for node instance '{}' before execution.", requestId, dagName, instanceName, e);
                Class<?> fallbackType = dagDefinition.getNodeOutputType(instanceName).orElse(Object.class);
                safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, Duration.ZERO, Duration.ZERO, e, null)); // node 为 null
                return Mono.just(NodeResult.failure(context, e, fallbackType));
            }

            Duration timeout = determineNodeTimeout(nodeImplementation);
            Instant startTime = Instant.now();
            safeNotifyListeners(l -> l.onNodeStart(requestId, dagName, instanceName, nodeImplementation));

            log.debug("[RequestId: {}] DAG '{}': Preparing execution for node '{}' (Impl: {}, Timeout: {})",
                    requestId, dagName, instanceName, nodeImplementation.getClass().getSimpleName(), timeout);

            // 2. 准备输入 (递归获取上游结果的 Mono)
            Mono<Map<String, NodeResult<C, ?>>> upstreamResultsMono = prepareInputs(
                    instanceName, context, cache, dagDefinition, requestId); // 传递 cache

            // 3. 执行或跳过
            return upstreamResultsMono
                    .flatMap(upstreamResults -> {
                        // 创建 InputAccessor，它会使用已完成的上游结果
                        InputAccessor<C> inputAccessor = new DefaultInputAccessor<>(instanceName, dagDefinition, upstreamResults);

                        boolean shouldExec;
                        try {
                            shouldExec = nodeImplementation.shouldExecute(context, inputAccessor);
                        } catch (Exception e) {
                            log.error("[RequestId: {}] DAG '{}': Node '{}' shouldExecute method threw exception. Treating as skipped/failed.",
                                    requestId, dagName, instanceName, e);
                            Duration totalDuration = Duration.between(startTime, Instant.now());
                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, totalDuration, Duration.ZERO, e, nodeImplementation));
                            return Mono.just(NodeResult.failure(context,
                                    new IllegalStateException("shouldExecute failed for node " + instanceName, e),
                                    outputPayloadType));
                        }

                        if (shouldExec) {
                            log.debug("[RequestId: {}] DAG '{}': Node '{}' condition met, proceeding to execute.", requestId, dagName, instanceName);
                            // 执行节点逻辑
                            Mono<NodeResult<C, ?>> resultMono = executeNodeInternal(
                                    nodeImplementation, context, inputAccessor, timeout, requestId, dagName, instanceName, startTime);
                            return resultMono;
                        } else {
                            log.debug("[RequestId: {}] DAG '{}': Node '{}' condition not met, skipping execution.", requestId, dagName, instanceName);
                            safeNotifyListeners(l -> l.onNodeSkipped(requestId, dagName, instanceName, nodeImplementation));
                            return Mono.just(NodeResult.skipped(context, outputPayloadType));
                        }
                    })
                    // 顶层错误处理 (例如输入准备失败)
                    .onErrorResume(error -> {
                        log.error("[RequestId: {}] DAG '{}': Node '{}' failed during input preparation or pre-execution: {}", requestId, dagName, instanceName, error.getMessage(), error);
                        Duration totalDuration = Duration.between(startTime, Instant.now());
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, totalDuration, Duration.ZERO, error, nodeImplementation));
                        return Mono.just(NodeResult.failure(context, error, outputPayloadType));
                    });
        }); // 结束 Mono.defer
    }

    // findNodeImplementation, determineNodeTimeout 保持不变

    /**
     * 准备节点的所有输入。
     * 返回一个 Mono，该 Mono 完成时会发出一个 Map，包含所有直接上游节点的实例名称及其对应的 NodeResult。
     * 它通过递归调用 getNodeExecutionMono 来获取上游节点的 Mono<NodeResult>。
     */
    private <C> Mono<Map<String, NodeResult<C, ?>>> prepareInputs(
            final String instanceName,
            final C context,
            final Cache<String, Mono<NodeResult<C, ?>>> cache, // 需要 cache 来获取上游 Mono
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();
        Map<String, String> wiring = dagDefinition.getUpstreamWiring(instanceName);

        if (wiring.isEmpty()) {
            return Mono.just(Collections.emptyMap());
        }

        Set<String> upstreamNodeNames = new HashSet<>(wiring.values());

        log.debug("[RequestId: {}] DAG '{}': Node '{}' preparing inputs from upstream nodes: {}",
                requestId, dagName, instanceName, upstreamNodeNames);

        // 为每个上游节点创建一个获取其 NodeResult 的 Mono
        // 使用 Flux.fromIterable + flatMap + collectMap 来并发获取所有上游结果
        return Flux.fromIterable(upstreamNodeNames)
                .flatMap(upstreamName ->
                        // 递归调用，获取上游节点的 Mono (可能来自缓存)
                        getNodeExecutionMono(upstreamName, context, cache, dagDefinition, requestId)
                                // 将结果 Mono 映射为包含节点名和结果的 Entry Mono
                                .map(result -> (Map.Entry<String, NodeResult<C, ?>>) new AbstractMap.SimpleEntry<>(upstreamName, result))
                                // 处理获取单个上游结果时发生的错误
                                .onErrorResume(e -> {
                                    log.error("[RequestId: {}] DAG '{}': Failed to get result for upstream node '{}' needed by '{}': {}",
                                            requestId, dagName, upstreamName, instanceName, e.getMessage());
                                    // 将错误包装后继续传播，以便 flatMap 收集失败
                                    return Mono.error(new RuntimeException(String.format("Failed to resolve dependency '%s' for node '%s'", upstreamName, instanceName), e));
                                })
                ) // flatMap 的结果是 Flux<Map.Entry<String, NodeResult<C, ?>>>
                .collectMap(Map.Entry::getKey, Map.Entry::getValue) // 收集成 Mono<Map<String, NodeResult<C, ?>>>
                .doOnSuccess(results -> log.debug(
                        "[RequestId: {}] DAG '{}': Node '{}' successfully prepared inputs from {} upstream nodes.",
                        requestId, dagName, instanceName, results.size()))
                .doOnError(e -> log.error(
                        "[RequestId: {}] DAG '{}': Node '{}' failed during input preparation: {}",
                        requestId, dagName, instanceName, e.getMessage(), e)); // 记录最终的输入准备错误
    }


    // executeNodeInternal, validateAndLogResult, safeNotifyListeners 保持不变
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
        DagNode rawNode = node; // 避免编译警告

        return Mono.deferContextual(contextView -> {
            Instant logicStartTime = Instant.now();
            log.debug("[RequestId: {}] DAG '{}': Node '{}' (Payload: {}, Impl: {}) core logic execution starting...",
                    requestId, dagName, instanceName, expectedPayloadType.getSimpleName(), node.getClass().getSimpleName());

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
                                        } else {
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
                        return Mono.just(NodeResult.failure(context, error, expectedPayloadType));
                    }))
                    .contextWrite(ctx -> ctx.put("logicStartTime", logicStartTime)); // 传递逻辑开始时间
        });
    }

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
            log.warn("[RequestId: {}] DAG '{}': Node '{}' (Expected Payload: {}) completed with non-SUCCESS status in onNext: {}",
                    requestId, dagName, instanceName, expectedPayloadType.getSimpleName(), result);
        }
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
