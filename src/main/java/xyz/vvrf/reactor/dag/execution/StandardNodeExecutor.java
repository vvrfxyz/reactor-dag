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
 * 获取节点实现，应用超时/重试（考虑实例特定覆盖），
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

    /**
     * 创建 StandardNodeExecutor 实例。
     * (通常由 DagEngineProvider 内部调用)。
     *
     * @param nodeRegistry           上下文 C 的 NodeRegistry。
     * @param defaultNodeTimeout     节点的全局默认超时时间。
     * @param nodeExecutionScheduler 节点执行的 Reactor Scheduler。
     * @param monitorListeners       监控监听器列表。
     */
    public StandardNodeExecutor(NodeRegistry<C> nodeRegistry,
                                Duration defaultNodeTimeout,
                                Scheduler nodeExecutionScheduler,
                                List<DagMonitorListener> monitorListeners) {
        this.nodeRegistry = Objects.requireNonNull(nodeRegistry, "NodeRegistry 不能为空");
        this.defaultNodeTimeout = Objects.requireNonNull(defaultNodeTimeout, "默认节点超时不能为空");
        this.nodeExecutionScheduler = Objects.requireNonNull(nodeExecutionScheduler, "节点执行调度器不能为空");
        this.monitorListeners = (monitorListeners != null) ? Collections.unmodifiableList(new ArrayList<>(monitorListeners)) : Collections.emptyList();
        this.contextType = nodeRegistry.getContextType(); // 从注册表获取上下文类型
        log.info("为上下文 '{}' 初始化了 StandardNodeExecutor。默认超时: {}, 调度器: {}, 监听器数量: {}",
                this.contextType.getSimpleName(),
                defaultNodeTimeout,
                nodeExecutionScheduler.getClass().getSimpleName(),
                this.monitorListeners.size());
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
                    DagNode<C, ?> nodeImplementation = null; // 在 try 外部声明以便 finally 和 catch 中使用
                    try {
                        // 1. 从注册表获取节点实现
                        nodeImplementation = nodeRegistry.getNodeInstance(nodeTypeId)
                                .orElseThrow(() -> new IllegalStateException(
                                        String.format("在注册表中找不到类型 ID '%s' (上下文: %s) 的节点实现",
                                                nodeTypeId, contextTypeName)));
                    } catch (Exception e) {
                        log.error("[RequestId: {}][DAG: {}][Context: {}] 从注册表获取节点实例 '{}' (类型: {}) 失败。",
                                requestId, dagName, contextTypeName, instanceName, nodeTypeId, e);
                        // 即使 nodeImplementation 为 null，也尝试通知，因为我们有 instanceName
                        final DagNode<?, ?> finalNodeImplForListener = nodeImplementation; // Effectively null here
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, Duration.ZERO, Duration.ZERO, e, finalNodeImplForListener));
                        return Mono.just(NodeResult.<C, Void>failure(e)); // 使用 Void 作为载荷类型
                    }

                    // 2. 确定有效的超时和重试策略，考虑实例覆盖
                    Duration effectiveTimeout = determineEffectiveTimeout(nodeDefinition, nodeImplementation);
                    Retry effectiveRetry = determineEffectiveRetry(nodeDefinition, nodeImplementation);

                    Instant startTime = Instant.now();
                    // 此处 dagName 已有值
                    DagNode<C, ?> finalNodeImplementation = nodeImplementation;
                    safeNotifyListeners(l -> l.onNodeStart(requestId, dagName, instanceName, finalNodeImplementation));

                    log.debug("[RequestId: {}][DAG: {}][Context: {}] 执行节点 '{}' (类型: {}, 实现: {}, 超时: {}, 重试: {})",
                            requestId, dagName, contextTypeName, instanceName, nodeTypeId, nodeImplementation.getClass().getSimpleName(),
                            effectiveTimeout, (effectiveRetry != null ? "自定义" : "节点默认"));

                    // 3. 使用有效设置执行内部逻辑
                    return executeNodeInternal(
                            nodeImplementation, context, inputAccessor,
                            effectiveTimeout, effectiveRetry,
                            requestId, dagName, instanceName, startTime, contextTypeName);

                })
                .onErrorResume(error -> { // 捕获设置阶段的错误（例如注册表查找失败后，这里是备用）
                    log.error("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 在预执行设置期间失败: {}",
                            requestId, dagName, contextTypeName, instanceName, error.getMessage(), error);
                    // 此时 nodeImplementation 可能为 null，监听器需要能处理
                    safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, Duration.ZERO, Duration.ZERO, error, null));
                    return Mono.just(NodeResult.<C, Void>failure(error));
                });
    }

    /**
     * 确定节点实例的有效超时时间。
     * 优先级：实例配置 -> 节点默认 -> 全局默认。
     */
    private Duration determineEffectiveTimeout(NodeDefinition nodeDefinition, DagNode<?, ?> nodeImplementation) {
        return nodeDefinition.getConfig(CONFIG_KEY_TIMEOUT, Duration.class)
                .orElseGet(() -> {
                    Duration nodeDefault = nodeImplementation.getExecutionTimeout();
                    return (nodeDefault != null && !nodeDefault.isZero() && !nodeDefault.isNegative())
                            ? nodeDefault
                            : this.defaultNodeTimeout;
                });
    }

    /**
     * 确定节点实例的有效重试策略。
     * 优先级：实例配置 -> 节点默认。
     */
    private Retry determineEffectiveRetry(NodeDefinition nodeDefinition, DagNode<?, ?> nodeImplementation) {
        Optional<Retry> instanceRetryOpt = nodeDefinition.getConfig(CONFIG_KEY_RETRY, Retry.class);
        if (instanceRetryOpt.isPresent()) {
            return instanceRetryOpt.get();
        } else {
            return nodeImplementation.getRetrySpec();
        }
    }


    /**
     * 带有超时、重试和结果/错误处理的内部执行逻辑。
     */
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

        DagNode rawNode = node;

        return Mono.deferContextual(contextView -> {
            Instant logicStartTime = Instant.now();
            log.trace("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' (实现: {}) 核心逻辑执行开始...",
                    requestId, dagName, contextTypeName, instanceName, node.getClass().getSimpleName());

            Mono<NodeResult<C, P>> executionMono = rawNode.execute(context, inputAccessor);

            return executionMono
                    .subscribeOn(nodeExecutionScheduler)
                    .timeout(timeout)
                    .doOnEach(signal -> {
                        signal.getContextView().<Instant>getOrEmpty("logicStartTime")
                                .ifPresent(lStartTime -> {
                                    Instant endTime = Instant.now();
                                    Duration totalDuration = Duration.between(startTime, endTime);
                                    Duration logicDuration = Duration.between(lStartTime, endTime);

                                    if (signal.isOnNext()) {
                                        NodeResult<C, P> result = (NodeResult<C, P>) signal.get();
                                        logResult(result, instanceName, requestId, dagName, contextTypeName);
                                        if (result.isSuccess()) {
                                            safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagName, instanceName, totalDuration, logicDuration, result, node));
                                        } else {
                                            Throwable err = result.getError().orElse(new RuntimeException("NodeResult 指示 FAILURE 但没有错误对象"));
                                            log.warn("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' execute() 返回了 FAILURE 状态。",
                                                    requestId, dagName, contextTypeName, instanceName);
                                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, totalDuration, logicDuration, err, node));
                                        }
                                    } else if (signal.isOnError()) {
                                        Throwable error = signal.getThrowable();
                                        if (error instanceof TimeoutException) {
                                            log.warn("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 执行尝试在 {} 后超时。",
                                                    requestId, dagName, contextTypeName, instanceName, timeout);
                                            safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagName, instanceName, timeout, node));
                                        } else {
                                            log.warn("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 执行尝试因异常失败: {}",
                                                    requestId, dagName, contextTypeName, instanceName, error.getMessage());
                                        }
                                    }
                                });
                    })
                    .retryWhen(retrySpec != null ? retrySpec : Retry.max(0))
                    .onErrorResume(error -> Mono.deferContextual(errorContextView -> {
                        Instant endTime = Instant.now();
                        Instant lStartTime = errorContextView.<Instant>getOrDefault("logicStartTime", startTime);
                        Duration totalDuration = Duration.between(startTime, endTime);
                        Duration logicDuration = Duration.between(lStartTime, endTime);

                        log.error("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 执行在重试/超时后最终失败: {}",
                                requestId, dagName, contextTypeName, instanceName, error.getMessage(), error);

                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, instanceName, totalDuration, logicDuration, error, node));
                        return Mono.just(NodeResult.<C, P>failure(error));
                    }))
                    .contextWrite(ctx -> ctx.put("logicStartTime", logicStartTime));
        });
    }

    private <P> void logResult(NodeResult<C, P> result, String instanceName, String requestId, String dagName, String contextTypeName) {
        if (result.isSuccess()) {
            log.debug("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 执行成功。结果状态: {}",
                    requestId, dagName, contextTypeName, instanceName, result.getStatus());
        } else {
            log.warn("[RequestId: {}][DAG: {}][Context: {}] 节点 '{}' 在 onNext 中以非 SUCCESS 状态完成: {}",
                    requestId, dagName, contextTypeName, instanceName, result.getStatus());
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
