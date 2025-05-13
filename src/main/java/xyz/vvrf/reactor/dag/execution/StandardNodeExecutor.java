package xyz.vvrf.reactor.dag.execution;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * NodeExecutor 的标准实现。
 * 获取节点实现，应用超时/重试（考虑实例特定覆盖和框架默认），
 * 执行节点逻辑，并处理结果/错误。
 *
 * @param <C> 上下文类型。
 * @author Refactored
 */
@Slf4j
public class StandardNodeExecutor<C> implements NodeExecutor<C> {

    /**
     * 用于在 NodeDefinition 中存储实例特定 Retry 规范的配置键。
     */
    public static final String CONFIG_KEY_RETRY = "__instanceRetry";
    /**
     * 用于在 NodeDefinition 中存储实例特定 Timeout 时长的配置键。
     */
    public static final String CONFIG_KEY_TIMEOUT = "__instanceTimeout";


    private final NodeRegistry<C> nodeRegistry;
    private final Duration defaultNodeTimeout;
    private final Scheduler nodeExecutionScheduler;
    private final List<DagMonitorListener> monitorListeners;
    private final Class<C> contextType;
    private final Retry frameworkDefaultRetrySpec; // 框架级别的默认重试策略

    /**
     * 创建 StandardNodeExecutor 实例。
     * (通常由 DagEngineProvider 内部调用)。
     *
     * @param nodeRegistry              上下文 C 的 NodeRegistry。
     * @param defaultNodeTimeout        节点的全局默认超时时间。
     * @param nodeExecutionScheduler    节点执行的 Reactor Scheduler。
     * @param monitorListeners          监控监听器列表。
     * @param frameworkDefaultRetrySpec 框架级别的默认重试策略 (可以为 Retry.max(0) 表示不重试)。
     */
    public StandardNodeExecutor(NodeRegistry<C> nodeRegistry,
                                Duration defaultNodeTimeout,
                                Scheduler nodeExecutionScheduler,
                                List<DagMonitorListener> monitorListeners,
                                Retry frameworkDefaultRetrySpec) {
        this.nodeRegistry = Objects.requireNonNull(nodeRegistry, "NodeRegistry 不能为空");
        this.defaultNodeTimeout = Objects.requireNonNull(defaultNodeTimeout, "默认节点超时不能为空");
        this.nodeExecutionScheduler = Objects.requireNonNull(nodeExecutionScheduler, "节点执行调度器不能为空");
        this.monitorListeners = (monitorListeners != null) ? Collections.unmodifiableList(new ArrayList<>(monitorListeners)) : Collections.emptyList();
        this.contextType = nodeRegistry.getContextType();
        this.frameworkDefaultRetrySpec = Objects.requireNonNull(frameworkDefaultRetrySpec, "框架默认重试策略不能为空");

        log.info("为上下文 '{}' 初始化了 StandardNodeExecutor。默认超时: {}, 调度器: {}, 监听器数量: {}, 框架默认重试: {}",
                this.contextType.getSimpleName(),
                defaultNodeTimeout,
                nodeExecutionScheduler.getClass().getSimpleName(),
                this.monitorListeners.size(),
                this.frameworkDefaultRetrySpec.getClass().getSimpleName());
    }

    @Override
    public Class<C> getContextType() {
        return this.contextType;
    }

    @Override
    public Mono<NodeResult<C, ?>> executeNode(
            NodeDefinition nodeDefinition,
            C context,
            InputAccessor<C> inputAccessor,
            String requestId,
            String dagName) {

        final String instanceName = nodeDefinition.getInstanceName();
        final String nodeTypeId = nodeDefinition.getNodeTypeId();
        final String contextTypeName = this.contextType.getSimpleName();

        return Mono.defer(() -> {
                    DagNode<C, ?> nodeImplementation = null;
                    try {
                        nodeImplementation = nodeRegistry.getNodeInstance(nodeTypeId)
                                .orElseThrow(() -> new IllegalStateException(
                                        String.format("在注册表中找不到类型 ID '%s' (上下文: %s) 的节点实现",
                                                nodeTypeId, contextTypeName)));
                    } catch (Exception e) {
                        log.error("[RequestId: {}][DAG: {}][Context: {}] 从注册表获取节点实例 '{}' (类型: {}) 失败。",
                                requestId, dagName, contextTypeName, instanceName, nodeTypeId, e);
                        final DagNode<?, ?> finalNodeImplForListener = nodeImplementation;
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, Duration.ZERO, Duration.ZERO, e, finalNodeImplForListener));
                        return Mono.just(NodeResult.<C, Void>failure(e));
                    }

                    Duration effectiveTimeout = determineEffectiveTimeout(nodeDefinition, nodeImplementation);
                    Retry effectiveRetry = determineEffectiveRetry(nodeDefinition, nodeImplementation);

                    Instant startTime = Instant.now();
                    DagNode<C, ?> finalNodeImplementation = nodeImplementation;
                    safeNotifyListeners(l -> l.onNodeStart(requestId, dagName, instanceName, finalNodeImplementation));

                    log.debug("[RequestId: {}][DAG: {}][Context: {}] 执行节点 '{}' (类型: {}, 实现: {}, 超时: {}, 重试: {})",
                            requestId, dagName, contextTypeName, instanceName, nodeTypeId, nodeImplementation.getClass().getSimpleName(),
                            effectiveTimeout, (effectiveRetry != Retry.max(0) ? "配置 (" + effectiveRetry.getClass().getSimpleName() + ")" : "不重试"));

                    return executeNodeInternal(
                            nodeImplementation, context, inputAccessor,
                            effectiveTimeout, effectiveRetry,
                            requestId, dagName, instanceName, startTime, contextTypeName);

                })
                .onErrorResume(error -> {
                    log.error("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 在预执行设置期间失败: {}",
                            requestId, dagName, contextTypeName, instanceName, error.getMessage(), error);
                    safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, Duration.ZERO, Duration.ZERO, error, null));
                    return Mono.just(NodeResult.<C, Void>failure(error));
                });
    }

    private Duration determineEffectiveTimeout(NodeDefinition nodeDefinition, DagNode<?, ?> nodeImplementation) {
        return nodeDefinition.getConfig(CONFIG_KEY_TIMEOUT, Duration.class)
                .orElseGet(() -> {
                    Duration nodeDefault = nodeImplementation.getExecutionTimeout();
                    return (nodeDefault != null && !nodeDefault.isZero() && !nodeDefault.isNegative())
                            ? nodeDefault
                            : this.defaultNodeTimeout;
                });
    }

    private Retry determineEffectiveRetry(NodeDefinition nodeDefinition, DagNode<?, ?> nodeImplementation) {
        // 1. Instance-specific config
        Optional<Retry> instanceRetryOpt = nodeDefinition.getConfig(CONFIG_KEY_RETRY, Retry.class);
        if (instanceRetryOpt.isPresent()) {
            log.debug("[Node: {}] 使用实例特定重试策略: {}", nodeDefinition.getInstanceName(), instanceRetryOpt.get().getClass().getSimpleName());
            return instanceRetryOpt.get();
        }

        // 2. Node-level default from DagNode implementation
        Retry nodeDefaultRetry = nodeImplementation.getRetrySpec();
        if (nodeDefaultRetry != null) {
            // If node explicitly provides a retry spec (even Retry.max(0) for no retry), use it.
            log.debug("[Node: {}] 使用节点实现提供的重试策略: {}", nodeDefinition.getInstanceName(), nodeDefaultRetry.getClass().getSimpleName());
            return nodeDefaultRetry;
        }

        // 3. Framework-level default
        if (this.frameworkDefaultRetrySpec != null) { // frameworkDefaultRetrySpec is guaranteed non-null by constructor
            log.debug("[Node: {}] 使用框架默认重试策略: {}", nodeDefinition.getInstanceName(), this.frameworkDefaultRetrySpec.getClass().getSimpleName());
            return this.frameworkDefaultRetrySpec;
        }

        // 4. Fallback: No retry (should not be reached if frameworkDefaultRetrySpec is always non-null e.g. Retry.max(0))
        log.debug("[Node: {}] 未配置重试策略，默认为不重试。", nodeDefinition.getInstanceName());
        return Retry.max(0);
    }


    @SuppressWarnings({"unchecked", "rawtypes"})
    private <P> Mono<NodeResult<C, P>> executeNodeInternal(
            DagNode<C, ?> node,
            C context,
            InputAccessor<C> inputAccessor,
            Duration timeout,
            Retry retrySpec,
            String requestId,
            String dagName,
            String instanceName,
            Instant startTime,
            String contextTypeName) {

        DagNode rawNode = node; // To call rawNode.execute

        return Mono.deferContextual(contextView -> {
            Instant logicStartTime = Instant.now();
            log.trace("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' (实现: {}) 核心逻辑执行开始...",
                    requestId, dagName, contextTypeName, instanceName, node.getClass().getSimpleName());

            Mono<NodeResult<C, P>> executionMono = rawNode.execute(context, inputAccessor);

            return executionMono
                    .subscribeOn(nodeExecutionScheduler)
                    .timeout(timeout)
                    .doOnEach(signal -> {
                        // Use contextView from deferContextual for logicStartTime
                        signal.getContextView().<Instant>getOrEmpty("logicStartTimeFromContext")
                                .ifPresent(lStartTime -> {
                                    Instant endTime = Instant.now();
                                    Duration totalDuration = Duration.between(startTime, endTime);
                                    Duration logicDuration = Duration.between(lStartTime, endTime);

                                    if (signal.isOnNext()) {
                                        NodeResult<C, P> result = (NodeResult<C, P>) signal.get();
                                        logResult(result, instanceName, requestId, dagName, contextTypeName);
                                        if (result.isSuccess()) {
                                            safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagName, instanceName, totalDuration, logicDuration, result, node));
                                        } else { // FAILURE from NodeResult
                                            Throwable err = result.getError().orElse(new RuntimeException("NodeResult 指示 FAILURE 但没有错误对象"));
                                            log.warn("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' execute() 返回了 FAILURE 状态。",
                                                    requestId, dagName, contextTypeName, instanceName);
                                            // Failure from NodeResult is not an "error" for retry purposes unless retry is configured to retry on specific NodeResult.
                                            // The current retry is for exceptions from the Mono.
                                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, totalDuration, logicDuration, err, node));
                                        }
                                    } else if (signal.isOnError()) { // Error from Mono (e.g. timeout, or exception in execute)
                                        Throwable error = signal.getThrowable();
                                        if (error instanceof TimeoutException) {
                                            log.warn("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 执行尝试在 {} 后超时。",
                                                    requestId, dagName, contextTypeName, instanceName, timeout);
                                            safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagName, instanceName, timeout, node));
                                            // onNodeFailure will be called by onErrorResume
                                        } else {
                                            log.warn("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 执行尝试因异常失败: {}",
                                                    requestId, dagName, contextTypeName, instanceName, error.getMessage());
                                            // onNodeFailure will be called by onErrorResume
                                        }
                                    }
                                });
                    })
                    .retryWhen(retrySpec) // retrySpec is guaranteed non-null (at least Retry.max(0))
                    .onErrorResume(error -> Mono.deferContextual(errorContextView -> { // Final failure after retries/timeout
                        Instant endTime = Instant.now();
                        Instant lStartTime = errorContextView.<Instant>getOrDefault("logicStartTimeFromContext", startTime); // Get from context
                        Duration totalDuration = Duration.between(startTime, endTime);
                        Duration logicDuration = Duration.between(lStartTime, endTime);

                        log.error("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 执行在重试/超时后最终失败: {}",
                                requestId, dagName, contextTypeName, instanceName, error.getMessage(), error);

                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, totalDuration, logicDuration, error, node));
                        return Mono.just(NodeResult.<C, P>failure(error));
                    }))
                    .contextWrite(ctx -> ctx.put("logicStartTimeFromContext", logicStartTime)); // Put logicStartTime into context
        });
    }

    private <P> void logResult(NodeResult<C, P> result, String instanceName, String requestId, String dagName, String contextTypeName) {
        if (result.isSuccess()) {
            log.debug("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 执行成功。结果状态: {}",
                    requestId, dagName, contextTypeName, instanceName, result.getStatus());
        } else { // This case is for NodeResult.failure() returned in onNext()
            log.warn("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 在 onNext 中以非 SUCCESS 状态完成: {} (错误: {})",
                    requestId, dagName, contextTypeName, instanceName, result.getStatus(), result.getError().map(Throwable::getMessage).orElse("N/A"));
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
                log.error("DAG 监控监听器 {} 在通知期间抛出异常: {}", listener.getClass().getName(), e.getMessage(), e);
            }
        }
    }
}
