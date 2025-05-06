// file: execution/StandardNodeExecutor.java
package xyz.vvrf.reactor.dag.execution;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;
import xyz.vvrf.reactor.dag.registry.NodeRegistry; // 依赖 NodeRegistry<C>

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * 标准节点执行器实现。
 * 负责获取节点实现、应用超时/重试、调用节点逻辑并处理结果/错误。
 *
 * @param <C> 上下文类型
 * @author Refactored
 */
@Slf4j
public class StandardNodeExecutor<C> implements NodeExecutor<C> {

    private final NodeRegistry<C> nodeRegistry; // 需要特定上下文的注册表
    private final Duration defaultNodeTimeout;
    private final Scheduler nodeExecutionScheduler;
    private final List<DagMonitorListener> monitorListeners; // 监听器是通用的
    private final Class<C> contextType;

    /**
     * 创建 StandardNodeExecutor 实例。
     *
     * @param nodeRegistry           用于获取节点实现的、特定于上下文 C 的 NodeRegistry。
     * @param defaultNodeTimeout     节点的默认超时时间。
     * @param nodeExecutionScheduler 用于执行节点逻辑的 Reactor Scheduler。
     * @param monitorListeners       用于监控节点事件的监听器列表 (通常由 Spring 自动收集)。
     */
    public StandardNodeExecutor(NodeRegistry<C> nodeRegistry,
                                Duration defaultNodeTimeout,
                                Scheduler nodeExecutionScheduler,
                                List<DagMonitorListener> monitorListeners) {
        this.nodeRegistry = Objects.requireNonNull(nodeRegistry, "NodeRegistry cannot be null");
        this.defaultNodeTimeout = Objects.requireNonNull(defaultNodeTimeout, "Default node timeout cannot be null");
        this.nodeExecutionScheduler = Objects.requireNonNull(nodeExecutionScheduler, "Node execution scheduler cannot be null");
        this.monitorListeners = (monitorListeners != null) ? Collections.unmodifiableList(new ArrayList<>(monitorListeners)) : Collections.emptyList();
        this.contextType = nodeRegistry.getContextType();
        log.info("StandardNodeExecutor for context '{}' initialized. Default timeout: {}, Scheduler: {}, Listeners: {}",
                nodeRegistry.getContextType().getSimpleName(), // 记录上下文类型
                defaultNodeTimeout,
                nodeExecutionScheduler.getClass().getSimpleName(),
                this.monitorListeners.size());
    }

    /**
     * 简化的构造函数，使用默认的 Schedulers.boundedElastic() 和空监听器列表。
     *
     * @param nodeRegistry       特定于上下文 C 的 NodeRegistry。
     * @param defaultNodeTimeout 节点的默认超时时间。
     */
    public StandardNodeExecutor(NodeRegistry<C> nodeRegistry, Duration defaultNodeTimeout) {
        this(nodeRegistry, defaultNodeTimeout, Schedulers.boundedElastic(), Collections.emptyList());
    }

    @Override
    public Class<C> getContextType() {
        return this.contextType; // 实现接口方法
    }

    @Override
    public Mono<NodeResult<C, ?>> executeNode(
            NodeDefinition nodeDefinition,
            C context,
            InputAccessor<C> inputAccessor,
            String requestId) {

        final String instanceName = nodeDefinition.getInstanceName();
        final String nodeTypeId = nodeDefinition.getNodeTypeId();
        final String contextTypeName = nodeRegistry.getContextType().getSimpleName(); // 获取上下文名称用于日志

        // 使用 Mono.defer 确保每次订阅时都执行查找和准备逻辑
        return Mono.defer(() -> {
                    // 1. 从注册表获取节点实例
                    DagNode<C, ?> nodeImplementation;
                    try {
                        // 使用注入的、特定上下文的 nodeRegistry
                        nodeImplementation = nodeRegistry.getNodeInstance(nodeTypeId)
                                .orElseThrow(() -> new IllegalStateException(
                                        String.format("Node implementation not found in registry for type ID '%s' (Context: %s)",
                                                nodeTypeId, contextTypeName)));
                    } catch (Exception e) {
                        log.error("[RequestId: {}][Context: {}] Failed to get node instance '{}' (Type: {}) from registry.",
                                requestId, contextTypeName, instanceName, nodeTypeId, e);
                        // 注意：dagName 在监听器中设为 null，因为执行器层面不直接持有 DAG 定义
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, null, instanceName, Duration.ZERO, Duration.ZERO, e, null));
                        // 显式转换为 Mono<NodeResult<C, ?>> 兼容的类型
                        return Mono.<NodeResult<C, ?>>just(NodeResult.failure(e));
                    }

                    // 2. 确定超时和开始时间
                    Duration timeout = determineNodeTimeout(nodeImplementation);
                    Instant startTime = Instant.now();
                    // 注意：dagName 在监听器中设为 null
                    safeNotifyListeners(l -> l.onNodeStart(requestId, null, instanceName, nodeImplementation));

                    log.debug("[RequestId: {}][Context: {}] Executing node '{}' (Type: {}, Impl: {}, Timeout: {})",
                            requestId, contextTypeName, instanceName, nodeTypeId, nodeImplementation.getClass().getSimpleName(), timeout);

                    // 3. 执行节点内部逻辑 (InputAccessor 已由引擎创建并传入)
                    // 需要强制转换类型以匹配 executeNodeInternal 的泛型参数 P
                    return executeNodeInternal(
                            nodeImplementation, context, inputAccessor,
                            timeout, requestId, instanceName, startTime, contextTypeName); // 传递 contextTypeName

                })
                .onErrorResume(error -> { // 处理获取节点实例时的错误 (例如上面的 catch 块抛出异常后)
                    log.error("[RequestId: {}][Context: {}] Node '{}' failed during pre-execution setup: {}",
                            requestId, contextTypeName, instanceName, error.getMessage(), error);
                    safeNotifyListeners(l -> l.onNodeFailure(requestId, null, instanceName, Duration.ZERO, Duration.ZERO, error, null));
                    // 返回失败结果
                    return Mono.just(NodeResult.<C, Void>failure(error)); // 使用 <C, Void>
                });
    }

    /**
     * 内部执行节点逻辑，处理超时、重试和结果/错误转换。
     * 使用了原始类型和 unchecked 转换来调用泛型的 execute 方法。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <P> Mono<NodeResult<C, P>> executeNodeInternal(
            DagNode<C, ?> node, // 接收通配符类型
            C context,
            InputAccessor<C> inputAccessor,
            Duration timeout,
            String requestId,
            String instanceName,
            Instant startTime,
            String contextTypeName) { // 接收 contextTypeName

        Retry retrySpec = node.getRetrySpec();
        DagNode rawNode = node; // 用于原始类型调用

        return Mono.deferContextual(contextView -> {
            Instant logicStartTime = Instant.now();
            log.debug("[RequestId: {}][Context: {}] Node '{}' (Impl: {}) core logic execution starting...",
                    requestId, contextTypeName, instanceName, node.getClass().getSimpleName());

            // 调用节点的 execute 方法 - 这里需要类型转换
            Mono<NodeResult<C, P>> executionMono = rawNode.execute(context, inputAccessor);

            return executionMono
                    .subscribeOn(nodeExecutionScheduler)
                    .timeout(timeout)
                    .doOnEach(signal -> { // 日志和监听器通知
                        signal.getContextView().<Instant>getOrEmpty("logicStartTime")
                                .ifPresent(lStartTime -> {
                                    Instant endTime = Instant.now();
                                    Duration totalDuration = Duration.between(startTime, endTime);
                                    Duration logicDuration = Duration.between(lStartTime, endTime);
                                    // 注意：dagName 设为 null
                                    String dagNameForListener = null;

                                    if (signal.isOnNext()) {
                                        NodeResult<C, P> result = (NodeResult<C, P>) signal.get();
                                        logResult(result, instanceName, requestId, contextTypeName); // 传递 contextTypeName
                                        if (result.isSuccess()) {
                                            safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagNameForListener, instanceName, totalDuration, logicDuration, result, node));
                                        } else {
                                            Throwable err = result.getError().orElse(new RuntimeException("NodeResult in onNext indicated failure with no error object"));
                                            log.warn("[RequestId: {}][Context: {}] Node '{}' execute() returned a non-SUCCESS NodeResult. Treating as failure.",
                                                    requestId, contextTypeName, instanceName);
                                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagNameForListener, instanceName, totalDuration, logicDuration, err, node));
                                        }
                                    } else if (signal.isOnError()) {
                                        Throwable error = signal.getThrowable();
                                        if (error instanceof TimeoutException) {
                                            log.warn("[RequestId: {}][Context: {}] Node '{}' execution attempt timed out after {}.",
                                                    requestId, contextTypeName, instanceName, timeout);
                                            safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagNameForListener, instanceName, timeout, node));
                                        } else {
                                            log.warn("[RequestId: {}][Context: {}] Node '{}' execution attempt failed: {}",
                                                    requestId, contextTypeName, instanceName, error.getMessage());
                                        }
                                        // 失败通知在下面的 onErrorResume 中处理
                                    }
                                });
                    })
                    .retryWhen(retrySpec != null ? retrySpec : Retry.max(0)) // 应用重试
                    .onErrorResume(error -> Mono.deferContextual(errorContextView -> { // 最终错误处理
                        Instant endTime = Instant.now();
                        Instant lStartTime = errorContextView.<Instant>getOrDefault("logicStartTime", startTime);
                        Duration totalDuration = Duration.between(startTime, endTime);
                        Duration logicDuration = Duration.between(lStartTime, endTime);
                        String dagNameForListener = null;

                        log.error("[RequestId: {}][Context: {}] Node '{}' execution ultimately failed: {}",
                                requestId, contextTypeName, instanceName, error.getMessage(), error);

                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagNameForListener, instanceName, totalDuration, logicDuration, error, node));
                        // 返回失败结果
                        return Mono.just(NodeResult.<C, P>failure(error)); // 保持泛型 P
                    }))
                    .contextWrite(ctx -> ctx.put("logicStartTime", logicStartTime));
        });
    }

    // 日志记录结果
    private <P> void logResult(NodeResult<C, P> result, String instanceName, String requestId, String contextTypeName) {
        if (result.isSuccess()) {
            log.debug("[RequestId: {}][Context: {}] Node '{}' executed successfully. Result: {}",
                    requestId, contextTypeName, instanceName, result);
        } else {
            log.warn("[RequestId: {}][Context: {}] Node '{}' completed with non-SUCCESS status in onNext: {}",
                    requestId, contextTypeName, instanceName, result);
        }
    }

    // 确定节点超时时间 (不变)
    private Duration determineNodeTimeout(DagNode<?, ?> node) {
        Duration nodeTimeout = node.getExecutionTimeout();
        return (nodeTimeout != null && !nodeTimeout.isZero() && !nodeTimeout.isNegative()) ? nodeTimeout : defaultNodeTimeout;
    }

    // 安全地通知监听器 (不变)
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
