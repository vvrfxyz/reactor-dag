// 文件名: execution/StandardNodeExecutor.java (已修改)
package xyz.vvrf.reactor.dag.execution;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
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

    // --- 配置键常量 ---
    /** 用于在 NodeDefinition 中存储实例特定 Retry 规范的配置键。 */
    public static final String CONFIG_KEY_RETRY = "__instanceRetry";
    /** 用于在 NodeDefinition 中存储实例特定 Timeout 时长的配置键。 */
    public static final String CONFIG_KEY_TIMEOUT = "__instanceTimeout";


    private final NodeRegistry<C> nodeRegistry;
    private final Duration defaultNodeTimeout; // 全局默认超时
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
            NodeDefinition nodeDefinition, // 现在包含实例配置
            C context,
            InputAccessor<C> inputAccessor,
            String requestId) {

        final String instanceName = nodeDefinition.getInstanceName();
        final String nodeTypeId = nodeDefinition.getNodeTypeId();
        final String contextTypeName = this.contextType.getSimpleName();

        return Mono.defer(() -> {
                    // 1. 从注册表获取节点实现
                    DagNode<C, ?> nodeImplementation;
                    try {
                        nodeImplementation = nodeRegistry.getNodeInstance(nodeTypeId)
                                .orElseThrow(() -> new IllegalStateException(
                                        String.format("在注册表中找不到类型 ID '%s' (上下文: %s) 的节点实现",
                                                nodeTypeId, contextTypeName)));
                    } catch (Exception e) {
                        log.error("[RequestId: {}][Context: {}] 从注册表获取节点实例 '{}' (类型: {}) 失败。",
                                requestId, contextTypeName, instanceName, nodeTypeId, e);
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, null, instanceName, Duration.ZERO, Duration.ZERO, e, null));
                        return Mono.just(NodeResult.<C, Void>failure(e)); // 使用 Void 作为载荷类型
                    }

                    // 2. 确定有效的超时和重试策略，考虑实例覆盖
                    Duration effectiveTimeout = determineEffectiveTimeout(nodeDefinition, nodeImplementation);
                    Retry effectiveRetry = determineEffectiveRetry(nodeDefinition, nodeImplementation);

                    Instant startTime = Instant.now();
                    safeNotifyListeners(l -> l.onNodeStart(requestId, null, instanceName, nodeImplementation)); // 此处 dagName 为 null

                    log.debug("[RequestId: {}][Context: {}] 执行节点 '{}' (类型: {}, 实现: {}, 超时: {}, 重试: {})",
                            requestId, contextTypeName, instanceName, nodeTypeId, nodeImplementation.getClass().getSimpleName(),
                            effectiveTimeout, (effectiveRetry != null ? "自定义" : "节点默认")); // 记录有效设置

                    // 3. 使用有效设置执行内部逻辑
                    return executeNodeInternal(
                            nodeImplementation, context, inputAccessor,
                            effectiveTimeout, effectiveRetry, // 传递有效设置
                            requestId, instanceName, startTime, contextTypeName);

                })
                .onErrorResume(error -> { // 捕获设置阶段的错误（例如注册表查找）
                    log.error("[RequestId: {}][Context: {}] 节点 '{}' 在预执行设置期间失败: {}",
                            requestId, contextTypeName, instanceName, error.getMessage(), error);
                    safeNotifyListeners(l -> l.onNodeFailure(requestId, null, instanceName, Duration.ZERO, Duration.ZERO, error, null));
                    return Mono.just(NodeResult.<C, Void>failure(error));
                });
    }

    /**
     * 确定节点实例的有效超时时间。
     * 优先级：实例配置 -> 节点默认 -> 全局默认。
     */
    private Duration determineEffectiveTimeout(NodeDefinition nodeDefinition, DagNode<?, ?> nodeImplementation) {
        return nodeDefinition.getConfig(CONFIG_KEY_TIMEOUT, Duration.class) // 首先检查实例配置
                .orElseGet(() -> { // 如果不存在，检查节点的默认值
                    Duration nodeDefault = nodeImplementation.getExecutionTimeout();
                    // 如果节点默认值有效，则使用它，否则使用全局默认值
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
        // 首先检查实例配置。如果存在，则使用它（即使为 null，表示覆盖为不重试）。
        // 如果配置中不存在，则使用节点的默认规范。
        Optional<Retry> instanceRetryOpt = nodeDefinition.getConfig(CONFIG_KEY_RETRY, Retry.class);
        if (instanceRetryOpt.isPresent()) {
            return instanceRetryOpt.get(); // 返回实例配置（可能为 null）
        } else {
            return nodeImplementation.getRetrySpec(); // 回退到节点的默认值
        }
    }


    /**
     * 带有超时、重试和结果/错误处理的内部执行逻辑。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <P> Mono<NodeResult<C, P>> executeNodeInternal(
            DagNode<C, ?> node, // 使用通配符类型
            C context,
            InputAccessor<C> inputAccessor,
            Duration timeout, // 使用有效超时
            Retry retrySpec,  // 使用有效重试
            String requestId,
            String instanceName,
            Instant startTime,
            String contextTypeName) {

        DagNode rawNode = node; // 用于原始类型调用

        return Mono.deferContextual(contextView -> {
            Instant logicStartTime = Instant.now();
            log.trace("[RequestId: {}][Context: {}] 节点 '{}' (实现: {}) 核心逻辑执行开始...",
                    requestId, contextTypeName, instanceName, node.getClass().getSimpleName());

            // 调用节点的 execute 方法
            Mono<NodeResult<C, P>> executionMono = rawNode.execute(context, inputAccessor);

            return executionMono
                    .subscribeOn(nodeExecutionScheduler) // 在指定调度器上执行
                    .timeout(timeout) // 应用有效超时
                    .doOnEach(signal -> { // 日志记录和监听器通知
                        signal.getContextView().<Instant>getOrEmpty("logicStartTime")
                                .ifPresent(lStartTime -> {
                                    Instant endTime = Instant.now();
                                    Duration totalDuration = Duration.between(startTime, endTime);
                                    Duration logicDuration = Duration.between(lStartTime, endTime);
                                    String dagNameForListener = null; // 执行器不知道 DAG 名称

                                    if (signal.isOnNext()) {
                                        NodeResult<C, P> result = (NodeResult<C, P>) signal.get(); // 安全转换
                                        logResult(result, instanceName, requestId, contextTypeName);
                                        if (result.isSuccess()) {
                                            safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagNameForListener, instanceName, totalDuration, logicDuration, result, node));
                                        } else { // 节点逻辑失败（返回 FAILURE 状态）
                                            Throwable err = result.getError().orElse(new RuntimeException("NodeResult 指示 FAILURE 但没有错误对象"));
                                            log.warn("[RequestId: {}][Context: {}] 节点 '{}' execute() 返回了 FAILURE 状态。",
                                                    requestId, contextTypeName, instanceName);
                                            // 失败通知在重试（如果有）后的 onErrorResume 中处理
                                            // 但我们可能需要一个特定的监听器事件来区分逻辑失败和异常？目前视为相同。
                                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagNameForListener, instanceName, totalDuration, logicDuration, err, node));
                                        }
                                    } else if (signal.isOnError()) {
                                        Throwable error = signal.getThrowable();
                                        if (error instanceof TimeoutException) {
                                            log.warn("[RequestId: {}][Context: {}] 节点 '{}' 执行尝试在 {} 后超时。",
                                                    requestId, contextTypeName, instanceName, timeout);
                                            safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagNameForListener, instanceName, timeout, node));
                                        } else {
                                            log.warn("[RequestId: {}][Context: {}] 节点 '{}' 执行尝试因异常失败: {}",
                                                    requestId, contextTypeName, instanceName, error.getMessage());
                                        }
                                        // 实际的失败通知延迟到 onErrorResume
                                    }
                                    // onComplete 信号通常由编排 Mono 的引擎处理
                                });
                    })
                    // 应用有效重试策略。如果为 null，则使用 Retry.max(0)。
                    .retryWhen(retrySpec != null ? retrySpec : Retry.max(0))
                    .onErrorResume(error -> Mono.deferContextual(errorContextView -> { // 超时/重试后的最终错误处理
                        Instant endTime = Instant.now();
                        Instant lStartTime = errorContextView.<Instant>getOrDefault("logicStartTime", startTime);
                        Duration totalDuration = Duration.between(startTime, endTime);
                        Duration logicDuration = Duration.between(lStartTime, endTime);
                        String dagNameForListener = null;

                        log.error("[RequestId: {}][Context: {}] 节点 '{}' 执行在重试/超时后最终失败: {}",
                                requestId, contextTypeName, instanceName, error.getMessage(), error);

                        // 通知监听器最终失败
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagNameForListener, instanceName, totalDuration, logicDuration, error, node));

                        // 返回 FAILURE 状态的 NodeResult
                        return Mono.just(NodeResult.<C, P>failure(error)); // 保持载荷类型 P
                    }))
                    .contextWrite(ctx -> ctx.put("logicStartTime", logicStartTime)); // 将逻辑开始时间添加到上下文
        });
    }

    // 记录结果状态的辅助方法
    private <P> void logResult(NodeResult<C, P> result, String instanceName, String requestId, String contextTypeName) {
        if (result.isSuccess()) {
            log.debug("[RequestId: {}][Context: {}] 节点 '{}' 执行成功。结果状态: {}",
                    requestId, contextTypeName, instanceName, result.getStatus());
        } else { // FAILURE 或 SKIPPED (尽管 SKIPPED 理想情况下不应通过 onNext 到达这里)
            log.warn("[RequestId: {}][Context: {}] 节点 '{}' 在 onNext 中以非 SUCCESS 状态完成: {}",
                    requestId, contextTypeName, instanceName, result.getStatus());
        }
    }

    // 安全通知监听器的辅助方法
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
