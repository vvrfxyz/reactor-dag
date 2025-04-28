package xyz.vvrf.reactor.dag.impl;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;

import java.time.Instant;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.InputDependencyAccessor;
import xyz.vvrf.reactor.dag.core.NodeResult;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
        this.monitorListeners = Collections.unmodifiableList(new ArrayList<>(
                Objects.requireNonNull(monitorListeners, "Monitor listeners list cannot be null")));

        if (defaultNodeTimeout.isNegative() || defaultNodeTimeout.isZero()) {
            log.warn("配置的 defaultNodeTimeout <= 0，节点执行可能不会超时: {}", defaultNodeTimeout);
        }

        log.info("StandardNodeExecutor 初始化完成，节点默认超时: {}, 调度器: {}, 监听器数量: {}",
                defaultNodeTimeout, nodeExecutionScheduler, this.monitorListeners.size());
    }
    // 保留其他构造函数
    public StandardNodeExecutor(Duration defaultNodeTimeout) {
        this(defaultNodeTimeout, Schedulers.boundedElastic(), Collections.emptyList());
    }
    @Deprecated
    public StandardNodeExecutor(Duration defaultNodeTimeout, Scheduler nodeExecutionScheduler) {
        this(defaultNodeTimeout, nodeExecutionScheduler, Collections.emptyList());
    }


    /**
     * 执行节点的核心逻辑（假设依赖已满足）。
     * 此方法由 StandardDagEngine 在依赖完成后调用。
     *
     * @param node            要执行的 DagNode 实例
     * @param context         当前上下文
     * @param accessor        已构建好的 InputDependencyAccessor
     * @param dagName         DAG 名称
     * @param requestId       请求 ID
     * @return 包含节点执行结果的 Mono<NodeResult>
     */
    public <C> Mono<NodeResult<C, ?, ?>> executeNodeCoreLogic(
            final DagNode<C, ?, ?> node,
            final C context,
            final InputDependencyAccessor<C> accessor, // 接收 Accessor
            final String dagName,
            final String requestId) {

        final String nodeName = node.getName();
        Instant nodeProcessingStartTime = Instant.now(); // 记录核心逻辑处理开始时间

        // 通知监听器节点开始处理 (核心逻辑部分)
        safeNotifyListeners(l -> l.onNodeStart(requestId, dagName, nodeName, node)); // 可能需要调整 Listener 接口或调用时机

        // 使用 Mono.deferContextual 可能更好，如果需要传递如开始时间等信息
        return Mono.defer(() -> {
                    // 1. 确定超时
                    Duration timeout = determineNodeTimeout(node);
                    log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Impl: {}) 开始执行核心逻辑, 超时: {}",
                            requestId, dagName, nodeName, node.getClass().getSimpleName(), timeout);

                    // 2. 检查 shouldExecute
                    boolean shouldExec;
                    Instant logicStartTime = Instant.now(); // shouldExecute 也算核心逻辑
                    try {
                        shouldExec = node.shouldExecute(context, accessor);
                    } catch (Exception e) {
                        log.error("[RequestId: {}] DAG '{}': 节点 '{}' 的 shouldExecute 方法抛出异常，将视为不执行并产生错误结果。",
                                requestId, dagName, nodeName, e);
                        Instant endTime = Instant.now();
                        Duration totalDuration = Duration.between(nodeProcessingStartTime, endTime);
                        Duration logicDuration = Duration.between(logicStartTime, endTime);
                        NodeResult<C, ?, ?> failureResult = NodeResult.failure(context, e, node);
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, e, node));
                        return Mono.just(failureResult);
                    }

                    // 3. 执行或跳过
                    if (shouldExec) {
                        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 条件满足，将执行。", requestId, dagName, nodeName);
                        // 调用内部执行方法
                        return executeNodeInternal(
                                node, context, accessor, timeout, requestId, dagName, nodeProcessingStartTime, logicStartTime);
                    } else {
                        log.info("[RequestId: {}] DAG '{}': 节点 '{}' 条件不满足，将被跳过。", requestId, dagName, nodeName);
                        Instant endTime = Instant.now();
                        Duration totalDuration = Duration.between(nodeProcessingStartTime, endTime);
                        NodeResult<C, ?, ?> skippedResult = NodeResult.skipped(context, node);
                        safeNotifyListeners(l -> l.onNodeSkipped(requestId, dagName, nodeName, node));
                        return Mono.just(skippedResult);
                    }
                })
                // 捕获 shouldExecute 或 execute 过程中的同步错误 (异步错误在 executeNodeInternal 中处理)
                .onErrorResume(error -> {
                    log.error("[RequestId: {}] DAG '{}': 节点 '{}' 核心逻辑准备阶段发生意外错误: {}", requestId, dagName, nodeName, error.getMessage(), error);
                    Instant endTime = Instant.now();
                    Duration totalDuration = Duration.between(nodeProcessingStartTime, endTime);
                    NodeResult<C, ?, ?> failureResult = NodeResult.failure(context, error, node);
                    safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, Duration.ZERO, error, node)); // Logic duration might be 0
                    return Mono.just(failureResult);
                });
    }


    /**
     * 内部方法：实际执行节点逻辑 (execute 方法调用、超时、重试)。
     */
    private <C> Mono<NodeResult<C, ?, ?>> executeNodeInternal(
            DagNode<C, ?, ?> node, // 接收通配符类型
            C context,
            InputDependencyAccessor<C> accessor,
            Duration timeout,
            String requestId,
            String dagName,
            Instant nodeProcessingStartTime, // 整个核心逻辑开始时间
            Instant logicStartTime) { // 实际 execute 调用开始时间 (或 shouldExecute 结束时间)

        String nodeName = node.getName();
        Retry retrySpec = node.getRetrySpec();

        // 使用 Mono.defer 确保每次订阅（或重试）时都调用 execute
        return Mono.defer(() -> {
                    log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Impl: {}) 调用 execute (或重试)...",
                            requestId, dagName, nodeName, node.getClass().getSimpleName());

                    // 调用节点的 execute 方法
                    @SuppressWarnings("unchecked") // 强制转换
                    DagNode<C, Object, Object> typedNode = (DagNode<C, Object, Object>) node;
                    Mono<NodeResult<C, Object, Object>> executionMono = typedNode.execute(context, accessor);

                    // 应用调度器、超时、日志、重试、错误处理
                    return executionMono
                            .subscribeOn(nodeExecutionScheduler)
                            .timeout(timeout)
                            .doOnSuccess(result -> { // 仅处理成功结果的日志和监控
                                Instant endTime = Instant.now();
                                Duration totalDuration = Duration.between(nodeProcessingStartTime, endTime);
                                Duration logicDuration = Duration.between(logicStartTime, endTime);
                                validateResultTypes(result, node, nodeName, dagName, requestId);
                                if (result.isSuccess()) {
                                    safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagName, nodeName, totalDuration, logicDuration, result, node));
                                } else {
                                    Throwable err = result.getError().orElseGet(() -> new NodeExecutionException("Node execute() returned a non-SUCCESS result without an error", nodeName, dagName));
                                    log.warn("[RequestId: {}] DAG '{}': 节点 '{}' execute() 成功返回但结果状态为 {}，按失败处理。", requestId, dagName, nodeName, result.getStatus());
                                    safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, err, node));
                                    // 注意：这里不会自动触发 onErrorResume，依赖于后续的错误处理
                                }
                            });
                })
                .retryWhen(retrySpec != null ? retrySpec : Retry.max(0))
                .doOnError(error -> {
                    if (!(error instanceof TimeoutException)) {
                        log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试失败 (非超时): {}", requestId, dagName, nodeName, error.getMessage());
                    }
                })
                .onErrorResume(error -> {
                    Instant endTime = Instant.now();
                    Duration totalDuration = Duration.between(nodeProcessingStartTime, endTime);
                    Duration logicDuration = Duration.between(logicStartTime, endTime);

                    NodeResult<C, Object, Object> failureResult = NodeResult.failureForNode(
                            context, error, Object.class, Object.class, nodeName
                    );

                    if (error instanceof TimeoutException) {
                        log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行最终超时 ({}).", requestId, dagName, nodeName, timeout);
                        safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagName, nodeName, timeout, node));
                    } else {
                        log.error("[RequestId: {}] DAG '{}': 节点 '{}' 执行最终失败: {}",
                                requestId, dagName, nodeName, error.getMessage(), error);
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, error, node));
                    }

                    return Mono.just(failureResult);
                })
                .map(result -> (NodeResult<C, ?, ?>) result); // 转回通配符类型
    }

    private <C> Duration determineNodeTimeout(DagNode<C, ?, ?> node) {
        Duration nodeTimeout = node.getExecutionTimeout();
        return (nodeTimeout != null && !nodeTimeout.isNegative() && !nodeTimeout.isZero())
                ? nodeTimeout
                : defaultNodeTimeout;
    }

    private <C> void validateResultTypes(
            NodeResult<C, ?, ?> result,
            DagNode<C, ?, ?> node,
            String nodeName,
            String dagName,
            String requestId) {
        // ... (实现不变) ...
        Class<?> declaredPayloadType = node.getPayloadType();
        Class<?> declaredEventType = node.getEventType();
        Class<?> actualPayloadType = result.getPayloadType();
        Class<?> actualEventType = result.getEventType();

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
        if (result.isSuccess()) {
            boolean isDefaultEmptyFlux = result.getEvents() == (Flux<?>) Flux.empty();
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Declared P: {}, E: {}) 执行成功. Result P: {}, E: {}, Payload: {}, IsDefaultEmptyFlux: {}",
                    requestId, dagName, nodeName,
                    declaredPayloadType.getSimpleName(), declaredEventType.getSimpleName(),
                    actualPayloadType != null ? actualPayloadType.getSimpleName() : "null",
                    actualEventType != null ? actualEventType.getSimpleName() : "null",
                    result.getPayload().isPresent() ? "Present" : "Empty",
                    isDefaultEmptyFlux ? "Yes" : "No");
        }
    }

    private void safeNotifyListeners(java.util.function.Consumer<DagMonitorListener> notification) {
        if (monitorListeners.isEmpty()) {
            return;
        }
        for (DagMonitorListener listener : monitorListeners) {
            try {
                notification.accept(listener);
            } catch (Exception e) {
                log.error("DAG Monitor Listener {} 在处理通知时抛出异常: {}", listener.getClass().getName(), e.getMessage(), e);
            }
        }
    }

    // --- 自定义异常类 (保持不变) ---
    static class NodeExecutionException extends RuntimeException {
        public NodeExecutionException(String message, String nodeName, String dagName) {
            super(String.format("[%s] Node '%s': %s", dagName, nodeName, message));
        }
    }
    static class NodeConfigurationException extends RuntimeException {
        public NodeConfigurationException(String message, Throwable cause, String nodeName, String dagName) {
            super(String.format("[%s] Node '%s' Configuration Error: %s", dagName, nodeName, message), cause);
        }
    }
}

