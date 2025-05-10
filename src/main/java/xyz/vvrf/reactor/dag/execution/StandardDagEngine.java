package xyz.vvrf.reactor.dag.execution;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DagEngine 的标准实现 。
 * 负责根据 DAG 定义编排节点执行，处理依赖关系、条件边和错误策略。
 * 每个 execute 调用创建一个 DagExecutionContext 来管理其状态。
 * 使用提供的 concurrencyLevel 控制引擎层面节点执行流的并发度。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen
 */
@Slf4j
public class StandardDagEngine<C> implements DagEngine<C> {

    private final NodeRegistry<C> nodeRegistry;
    private final NodeExecutor<C> nodeExecutor;
    private final int concurrencyLevel;
    private final List<DagMonitorListener> monitorListeners;

    public StandardDagEngine(NodeRegistry<C> nodeRegistry,
                             NodeExecutor<C> nodeExecutor,
                             int concurrencyLevel,
                             List<DagMonitorListener> monitorListeners) { // 新增参数
        this.nodeRegistry = Objects.requireNonNull(nodeRegistry, "NodeRegistry cannot be null");
        this.nodeExecutor = Objects.requireNonNull(nodeExecutor, "NodeExecutor cannot be null");
        if (concurrencyLevel <= 0) {
            throw new IllegalArgumentException("Concurrency level must be positive.");
        }
        this.concurrencyLevel = concurrencyLevel;
        this.monitorListeners = (monitorListeners != null) ? Collections.unmodifiableList(new ArrayList<>(monitorListeners)) : Collections.emptyList(); // 初始化监听器
        log.info("StandardDagEngine for context '{}' initialized. Executor: {}, Concurrency Level: {}, Listeners: {}",
                nodeRegistry.getContextType().getSimpleName(),
                nodeExecutor.getClass().getSimpleName(),
                this.concurrencyLevel,
                this.monitorListeners.size());
    }

    // 为了兼容旧的构造函数（如果外部有直接调用），可以保留或提供一个重载
    // 但通常由 DagEngineProvider 创建，它会传递所有参数
    public StandardDagEngine(NodeRegistry<C> nodeRegistry, NodeExecutor<C> nodeExecutor, int concurrencyLevel) {
        this(nodeRegistry, nodeExecutor, concurrencyLevel, Collections.emptyList());
        log.warn("StandardDagEngine created without explicit monitor listeners. DAG level monitoring might be limited.");
    }


    @Override
    public Flux<Event<?>> execute(C initialContext, DagDefinition<C> dagDefinition, String requestId) {
        final DagExecutionContext<C> executionContext = new DagExecutionContext<>(initialContext, dagDefinition, requestId);

        final String actualRequestId = executionContext.getRequestId();
        final String dagName = executionContext.getDagName();
        final int totalNodes = executionContext.getTotalNodes();

        // DAG 开始事件
        safeNotifyListeners(l -> l.onDagStart(actualRequestId, dagName, dagDefinition, initialContext));

        if (totalNodes == 0) {
            log.info("[RequestId: {}][DAG: '{}'] DAG is empty, returning empty Flux.", actualRequestId, dagName);
            // DAG 完成事件 (空 DAG)
            Map<String, NodeResult<?, ?>> finalResultsView = Collections.unmodifiableMap(new HashMap<>(executionContext.getCompletedResults()));
            safeNotifyListeners(l -> l.onDagComplete(actualRequestId, dagName, dagDefinition, Duration.between(executionContext.getDagStartTime(), Instant.now()), true, finalResultsView, null));
            return Flux.empty();
        }

        return Flux.defer(() -> {
            log.debug("[RequestId: {}][DAG: '{}'] Initializing execution for all {} nodes with concurrency level {}.",
                    actualRequestId, dagName, totalNodes, this.concurrencyLevel);

            return Flux.fromIterable(executionContext.getAllNodeNames())
                    .flatMap(nodeName -> getNodeMono(nodeName, executionContext)
                                    .then()
                            , this.concurrencyLevel)
                    .then()
                    .doOnSubscribe(s -> log.debug("[RequestId: {}][DAG: '{}'] Main execution Flux subscribed, processing nodes with concurrency {}.",
                            actualRequestId, dagName, this.concurrencyLevel))
                    .thenMany(Flux.defer(() -> {
                        log.debug("[RequestId: {}][DAG: '{}'] All node flows completed. Extracting events from successful results.", actualRequestId, dagName);
                        return Flux.fromIterable(executionContext.getCompletedResults().values())
                                .filter(NodeResult::isSuccess)
                                .flatMap(NodeResult::getEvents);
                    }))
                    .doOnComplete(() -> {
                        logCompletion(executionContext);
                        // DAG 完成事件 (成功)
                        Map<String, NodeResult<?, ?>> finalResultsView = Collections.unmodifiableMap(new HashMap<>(executionContext.getCompletedResults()));
                        safeNotifyListeners(l -> l.onDagComplete(actualRequestId, dagName, dagDefinition, Duration.between(executionContext.getDagStartTime(), Instant.now()), executionContext.isOverallSuccess(), finalResultsView, null));
                    })
                    .doOnError(e -> {
                        log.error("[RequestId: {}][DAG: '{}'] Execution failed with unexpected error in main stream: {}", actualRequestId, dagName, e.getMessage(), e);
                        executionContext.setOverallFailure();
                        logCompletion(executionContext);
                        // DAG 完成事件 (失败)
                        Map<String, NodeResult<?, ?>> finalResultsView = Collections.unmodifiableMap(new HashMap<>(executionContext.getCompletedResults()));
                        safeNotifyListeners(l -> l.onDagComplete(actualRequestId, dagName, dagDefinition, Duration.between(executionContext.getDagStartTime(), Instant.now()), false, finalResultsView, e));
                    })
                    .doFinally(signal -> {
                        log.debug("[RequestId: {}][DAG: '{}'] Execution finished (Signal: {}). Cleaning up execution mono cache.", actualRequestId, dagName, signal);
                        executionContext.clearExecutionMonoCache();
                    });
        });
    }

    private Mono<NodeResult<C, ?>> getNodeMono(
            String instanceName,
            DagExecutionContext<C> executionContext) {

        return executionContext.getOrCreateNodeMono(instanceName, () ->
                Mono.defer(() -> {
                            log.trace("[RequestId: {}][DAG: '{}'] Subscription received for node '{}'. Evaluating dependencies and execution status.",
                                    executionContext.getRequestId(), executionContext.getDagName(), instanceName);

                            if (executionContext.getErrorStrategy() == ErrorHandlingStrategy.FAIL_FAST && executionContext.isFailFastActive()) {
                                log.debug("[RequestId: {}][DAG: '{}'] FAIL_FAST active, skipping node '{}' (cached check).",
                                        executionContext.getRequestId(), executionContext.getDagName(), instanceName);
                                return createSkippedResultMono(instanceName, executionContext);
                            }

                            List<EdgeDefinition<C>> incomingEdges = executionContext.getDagDefinition().getIncomingEdges(instanceName);
                            List<Mono<NodeResult<C, ?>>> dependencyMonos;

                            if (incomingEdges.isEmpty()) {
                                dependencyMonos = Collections.emptyList();
                                log.trace("[RequestId: {}][DAG: '{}'] Node '{}' has no dependencies.",
                                        executionContext.getRequestId(), executionContext.getDagName(), instanceName);
                            } else {
                                dependencyMonos = incomingEdges.stream()
                                        .map(EdgeDefinition::getUpstreamInstanceName)
                                        .distinct()
                                        .map(upstreamName -> {
                                            log.trace("[RequestId: {}][DAG: '{}'] Node '{}' depends on '{}'. Getting/creating dependency Mono.",
                                                    executionContext.getRequestId(), executionContext.getDagName(), instanceName, upstreamName);
                                            return getNodeMono(upstreamName, executionContext);
                                        })
                                        .collect(Collectors.toList());
                            }

                            return Mono.when(dependencyMonos)
                                    .doOnSubscribe(s -> log.trace("[RequestId: {}][DAG: '{}'] Subscribed to dependencies for node '{}'. Waiting...",
                                            executionContext.getRequestId(), executionContext.getDagName(), instanceName))
                                    .then(Mono.defer(() -> {
                                        log.trace("[RequestId: {}][DAG: '{}'] Dependencies for node '{}' met. Proceeding to evaluate conditions and execute.",
                                                executionContext.getRequestId(), executionContext.getDagName(), instanceName);

                                        if (executionContext.getErrorStrategy() == ErrorHandlingStrategy.FAIL_FAST && executionContext.isFailFastActive()) {
                                            log.debug("[RequestId: {}][DAG: '{}'] FAIL_FAST active after dependencies resolved, skipping node '{}'.",
                                                    executionContext.getRequestId(), executionContext.getDagName(), instanceName);
                                            return createSkippedResultMono(instanceName, executionContext);
                                        }

                                        List<EdgeDefinition<C>> activeEdges = new ArrayList<>();
                                        boolean canExecute = false;
                                        NodeDefinition nodeDef = executionContext.getDagDefinition().getNodeDefinition(instanceName)
                                                .orElseThrow(() -> new IllegalStateException("Node definition disappeared for: " + instanceName));

                                        if (incomingEdges.isEmpty()) {
                                            canExecute = true;
                                            log.trace("[RequestId: {}][DAG: '{}'] Node '{}' is a source node, can execute.",
                                                    executionContext.getRequestId(), executionContext.getDagName(), instanceName);
                                        } else {
                                            boolean upstreamErrorInFailFast = false;
                                            for (EdgeDefinition<C> edge : incomingEdges) {
                                                String upstreamName = edge.getUpstreamInstanceName();
                                                NodeResult<C, ?> upstreamResult = executionContext.getCompletedResults().get(upstreamName);

                                                if (upstreamResult == null) {
                                                    String errorMsg = String.format("Consistency error: Upstream node '%s' result not found for edge to '%s', though dependencies should be met.", upstreamName, instanceName);
                                                    log.error("[RequestId: {}][DAG: '{}'] {}", executionContext.getRequestId(), executionContext.getDagName(), errorMsg);
                                                    handleFailure(instanceName, new IllegalStateException(errorMsg), executionContext);
                                                    return Mono.just(executionContext.getCompletedResults().get(instanceName));
                                                }

                                                log.trace("[RequestId: {}][DAG: '{}'] Checking edge {} -> {}. Upstream status: {}",
                                                        executionContext.getRequestId(), executionContext.getDagName(), upstreamName, instanceName, upstreamResult.getStatus());

                                                if (upstreamResult.isSuccess()) {
                                                    boolean conditionMet = false;
                                                    ConditionBase<C> condition = edge.getCondition();
                                                    String conditionTypeName = condition.getClass().getSimpleName();

                                                    try {
                                                        if (condition instanceof DirectUpstreamCondition) {
                                                            @SuppressWarnings({"rawtypes", "unchecked"})
                                                            DirectUpstreamCondition rawDirectCond = (DirectUpstreamCondition) condition;
                                                            conditionMet = rawDirectCond.evaluate(executionContext.getInitialContext(), upstreamResult);
                                                            log.trace("[RequestId: {}][DAG: '{}'] Evaluated DirectUpstreamCondition for edge {} -> {}: {}", executionContext.getRequestId(), executionContext.getDagName(), upstreamName, instanceName, conditionMet);
                                                        } else if (condition instanceof LocalInputCondition) {
                                                            LocalInputCondition<C> localCond = (LocalInputCondition<C>) condition;
                                                            LocalInputAccessor<C> localAccessor = new xyz.vvrf.reactor.dag.execution.DefaultLocalInputAccessor<>(executionContext.getCompletedResults(), executionContext.getDagDefinition(), instanceName);
                                                            conditionMet = localCond.evaluate(executionContext.getInitialContext(), localAccessor);
                                                            log.trace("[RequestId: {}][DAG: '{}'] Evaluated LocalInputCondition for edge {} -> {}: {}", executionContext.getRequestId(), executionContext.getDagName(), upstreamName, instanceName, conditionMet);
                                                        } else if (condition instanceof DeclaredDependencyCondition) {
                                                            DeclaredDependencyCondition<C> declaredCond = (DeclaredDependencyCondition<C>) condition;
                                                            Set<String> explicitDeps = declaredCond.getRequiredNodeDependencies();
                                                            Set<String> allowedNodes = new HashSet<>(explicitDeps);
                                                            allowedNodes.add(upstreamName);
                                                            ConditionInputAccessor<C> restrictedAccessor = new RestrictedConditionInputAccessor<>(executionContext.getCompletedResults(), allowedNodes, upstreamName);
                                                            conditionMet = declaredCond.evaluate(executionContext.getInitialContext(), restrictedAccessor);
                                                            log.trace("[RequestId: {}][DAG: '{}'] Evaluated DeclaredDependencyCondition for edge {} -> {} (Allowed: {}): {}", executionContext.getRequestId(), executionContext.getDagName(), upstreamName, instanceName, allowedNodes, conditionMet);
                                                        } else {
                                                            log.error("[RequestId: {}][DAG: '{}'] Unknown condition type '{}' for edge {} -> {}. Treating as false.", executionContext.getRequestId(), executionContext.getDagName(), condition.getClass().getName(), upstreamName, instanceName);
                                                            conditionMet = false;
                                                        }
                                                    } catch (IllegalArgumentException accessError) {
                                                        log.error("[RequestId: {}][DAG: '{}'] Condition ({}) evaluation for edge {} -> {} failed due to restricted access: {}. Treating as node failure.", executionContext.getRequestId(), executionContext.getDagName(), conditionTypeName, upstreamName, instanceName, accessError.getMessage());
                                                        handleFailure(instanceName, accessError, executionContext);
                                                        return Mono.just(executionContext.getCompletedResults().get(instanceName));
                                                    } catch (Exception e) {
                                                        log.error("[RequestId: {}][DAG: '{}'] Condition ({}) evaluation failed unexpectedly for edge {} -> {}. Treating as node failure.", executionContext.getRequestId(), executionContext.getDagName(), conditionTypeName, upstreamName, instanceName, e);
                                                        handleFailure(instanceName, e, executionContext);
                                                        return Mono.just(executionContext.getCompletedResults().get(instanceName));
                                                    }

                                                    if (conditionMet) {
                                                        activeEdges.add(edge);
                                                        canExecute = true;
                                                    }
                                                } else if (upstreamResult.isFailure()) {
                                                    if (executionContext.getErrorStrategy() == ErrorHandlingStrategy.FAIL_FAST) {
                                                        upstreamErrorInFailFast = true;
                                                        log.debug("[RequestId: {}][DAG: '{}'] Upstream node '{}' failed in FAIL_FAST mode. Node '{}' will be skipped.", executionContext.getRequestId(), executionContext.getDagName(), upstreamName, instanceName);
                                                        break;
                                                    } else {
                                                        log.debug("[RequestId: {}][DAG: '{}'] Upstream node '{}' failed in CONTINUE_ON_FAILURE mode. Node '{}' might still execute if other inputs are available.", executionContext.getRequestId(), executionContext.getDagName(), upstreamName, instanceName);
                                                    }
                                                } else if (upstreamResult.isSkipped()) {
                                                    log.debug("[RequestId: {}][DAG: '{}'] Upstream node '{}' was skipped. Node '{}' might be skipped if all inputs are skipped or conditions not met.", executionContext.getRequestId(), executionContext.getDagName(), upstreamName, instanceName);
                                                }
                                            }
                                            if (upstreamErrorInFailFast) {
                                                canExecute = false;
                                            }
                                        }

                                        Mono<NodeResult<C, ?>> executionOrSkipMono;
                                        if (canExecute) {
                                            log.debug("[RequestId: {}][DAG: '{}'] Conditions met for node '{}'. Preparing to execute via NodeExecutor. Active Edges: {}",
                                                    executionContext.getRequestId(), executionContext.getDagName(), instanceName, activeEdges.stream().map(e -> e.getUpstreamInstanceName() + "->" + e.getDownstreamInstanceName()).collect(Collectors.joining(", ")));

                                            InputAccessor<C> inputAccessor;
                                            try {
                                                NodeRegistry.NodeMetadata meta = nodeRegistry.getNodeMetadata(nodeDef.getNodeTypeId())
                                                        .orElseThrow(() -> new IllegalStateException("Metadata not found for " + nodeDef.getNodeTypeId() + " in registry " + nodeRegistry.getClass().getSimpleName()));
                                                Set<InputSlot<?>> declaredInputSlots = meta.getInputSlots();
                                                inputAccessor = new DefaultInputAccessor<>(declaredInputSlots, incomingEdges, executionContext.getCompletedResults(), activeEdges);
                                            } catch (Exception e) {
                                                log.error("[RequestId: {}][DAG: '{}'] Failed to create InputAccessor for node '{}'. Treating as node failure.",
                                                        executionContext.getRequestId(), executionContext.getDagName(), instanceName, e);
                                                handleFailure(instanceName, e, executionContext);
                                                return Mono.just(executionContext.getCompletedResults().get(instanceName));
                                            }
                                            // 调用 NodeExecutor, 传递 dagName
                                            executionOrSkipMono = nodeExecutor.executeNode(nodeDef, executionContext.getInitialContext(), inputAccessor, executionContext.getRequestId(), executionContext.getDagName());
                                        } else {
                                            log.debug("[RequestId: {}][DAG: '{}'] Conditions not met or critical dependencies failed/skipped. Skipping node '{}'.",
                                                    executionContext.getRequestId(), executionContext.getDagName(), instanceName);
                                            executionOrSkipMono = createSkippedResultMono(instanceName, executionContext);
                                        }

                                        return executionOrSkipMono
                                                .doOnNext(result -> {
                                                    if (executionContext.recordCompletedResult(instanceName, result)) {
                                                        if (result.isFailure()) {
                                                            handleFailureInternal(instanceName, result.getError().orElse(new RuntimeException("Unknown failure")), executionContext);
                                                        }
                                                    }
                                                })
                                                .doOnError(error -> {
                                                    log.error("[RequestId: {}][DAG: '{}'] Unexpected error during execution/skip processing for node '{}'. Recording failure.",
                                                            executionContext.getRequestId(), executionContext.getDagName(), instanceName, error);
                                                    handleFailure(instanceName, error, executionContext);
                                                });
                                    }));
                        })
                        .cache()
        );
    }


    private Mono<NodeResult<C, ?>> createSkippedResultMono(String instanceName, DagExecutionContext<C> context) {
        NodeResult<C, ?> skippedResult = NodeResult.skipped();
        context.recordCompletedResult(instanceName, skippedResult);

        // 获取节点定义以传递给监听器
        DagNode<C, ?> nodeImplementation = null;
        try {
            nodeImplementation = context.getDagDefinition().getNodeDefinition(instanceName)
                    .flatMap(nodeDef -> nodeRegistry.getNodeInstance(nodeDef.getNodeTypeId()))
                    .orElse(null);
        } catch (Exception e) {
            log.warn("[RequestId: {}][DAG: '{}'] Error retrieving node implementation for skipped node '{}' for monitoring: {}",
                    context.getRequestId(), context.getDagName(), instanceName, e.getMessage());
        }
        final DagNode<C, ?> finalNodeImpl = nodeImplementation;
        safeNotifyListeners(l -> l.onNodeSkipped(context.getRequestId(), context.getDagName(), instanceName, finalNodeImpl));
        return Mono.just(skippedResult);
    }

    private void handleFailure(String instanceName, Throwable error, DagExecutionContext<C> context) {
        NodeResult<C, ?> failureResult = NodeResult.failure(error);
        if (context.recordCompletedResult(instanceName, failureResult)) {
            handleFailureInternal(instanceName, error, context);
        } else {
            log.warn("[RequestId: {}][DAG: '{}'] Node '{}' result ALREADY recorded when trying to mark as FAILURE. Error: {}.",
                    context.getRequestId(), context.getDagName(), instanceName, error.getMessage());
        }
    }

    private void handleFailureInternal(String instanceName, Throwable error, DagExecutionContext<C> context) {
        context.setOverallFailure();
        if (context.getErrorStrategy() == ErrorHandlingStrategy.FAIL_FAST) {
            if (context.triggerFailFast()) {
                log.debug("[RequestId: {}][DAG: '{}'] Node '{}' failed. FAIL_FAST strategy activated. Error: {}",
                        context.getRequestId(), context.getDagName(), instanceName, error.getMessage());
            } else {
                log.debug("[RequestId: {}][DAG: '{}'] Node '{}' failed, but FAIL_FAST was already active. Error: {}",
                        context.getRequestId(), context.getDagName(), instanceName, error.getMessage());
            }
        } else {
            log.debug("[RequestId: {}][DAG: '{}'] Node '{}' failed (CONTINUE_ON_FAILURE strategy). Execution continues. Error: {}",
                    context.getRequestId(), context.getDagName(), instanceName, error.getMessage());
        }
    }

    private void logCompletion(DagExecutionContext<C> context) {
        String finalStatus;
        if (context.getErrorStrategy() == ErrorHandlingStrategy.FAIL_FAST) {
            finalStatus = context.isFailFastActive() ? "FAILED (FAIL_FAST)" : (context.isOverallSuccess() ? "SUCCESS" : "FAILED");
        } else {
            finalStatus = context.isOverallSuccess() ? "SUCCESS" : "COMPLETED_WITH_FAILURES";
        }
        boolean allAccountedFor = (context.getCompletedNodeCount() == context.getTotalNodes());
        log.info("[RequestId: {}][DAG: '{}'][Context: {}] Execution finished. Final Status: {}. Completed nodes accounted for: {}/{}. All nodes processed: {}",
                context.getRequestId(), context.getDagName(), context.getContextTypeName(),
                finalStatus, context.getCompletedNodeCount(), context.getTotalNodes(), allAccountedFor);

        if (!allAccountedFor) {
            log.warn("[RequestId: {}][DAG: '{}'] Potential issue: Not all nodes ({}) were accounted for upon completion ({}). This might indicate an issue in the execution logic or graph structure.",
                    context.getRequestId(), context.getDagName(), context.getTotalNodes(), context.getCompletedNodeCount());
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
