package xyz.vvrf.reactor.dag.execution;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;

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

    public StandardDagEngine(NodeRegistry<C> nodeRegistry, NodeExecutor<C> nodeExecutor, int concurrencyLevel) {
        this.nodeRegistry = Objects.requireNonNull(nodeRegistry, "NodeRegistry cannot be null");
        this.nodeExecutor = Objects.requireNonNull(nodeExecutor, "NodeExecutor cannot be null");
        if (concurrencyLevel <= 0) {
            throw new IllegalArgumentException("Concurrency level must be positive.");
        }
        this.concurrencyLevel = concurrencyLevel;
        log.info("StandardDagEngine for context '{}' initialized. Executor: {}, Concurrency Level: {}",
                nodeRegistry.getContextType().getSimpleName(),
                nodeExecutor.getClass().getSimpleName(),
                this.concurrencyLevel);
    }

    @Override
    public Flux<Event<?>> execute(C initialContext, DagDefinition<C> dagDefinition, String requestId) {
        // 1. 创建 DagExecutionContext 来管理此执行的状态
        final DagExecutionContext<C> executionContext = new DagExecutionContext<>(initialContext, dagDefinition, requestId);

        final String actualRequestId = executionContext.getRequestId();
        final String dagName = executionContext.getDagName();
        final int totalNodes = executionContext.getTotalNodes();

        if (totalNodes == 0) {
            log.info("[RequestId: {}][DAG: '{}'] DAG is empty, returning empty Flux.", actualRequestId, dagName);
            return Flux.empty();
        }

        return Flux.defer(() -> {
            log.debug("[RequestId: {}][DAG: '{}'] Initializing execution for all {} nodes with concurrency level {}.",
                    actualRequestId, dagName, totalNodes, this.concurrencyLevel);

            // 使用 Flux.flatMap 控制并发执行 getNodeMono
            return Flux.fromIterable(executionContext.getAllNodeNames())
                    .flatMap(nodeName -> getNodeMono(nodeName, executionContext)
                                    .then() // 我们只需要完成信号，结果通过 executionContext.completedResults 共享
                            , this.concurrencyLevel) // 应用并发控制
                    .then() // 等待所有 flatMap 中的 Mono 完成，得到一个 Mono<Void>
                    .doOnSubscribe(s -> log.debug("[RequestId: {}][DAG: '{}'] Main execution Flux subscribed, processing nodes with concurrency {}.",
                            actualRequestId, dagName, this.concurrencyLevel))
                    .thenMany(Flux.defer(() -> { // 在所有节点处理完成后（成功、失败或跳过），收集事件
                        log.debug("[RequestId: {}][DAG: '{}'] All node flows completed. Extracting events from successful results.", actualRequestId, dagName);
                        return Flux.fromIterable(executionContext.getCompletedResults().values())
                                .filter(NodeResult::isSuccess)
                                .flatMap(NodeResult::getEvents);
                    }))
                    .doOnComplete(() -> logCompletion(executionContext))
                    .doOnError(e -> {
                        log.error("[RequestId: {}][DAG: '{}'] Execution failed with unexpected error in main stream: {}", actualRequestId, dagName, e.getMessage(), e);
                        executionContext.setOverallFailure(); // 标记总体失败
                        logCompletion(executionContext);
                    })
                    .doFinally(signal -> {
                        log.debug("[RequestId: {}][DAG: '{}'] Execution finished (Signal: {}). Cleaning up execution mono cache.", actualRequestId, dagName, signal);
                        executionContext.clearExecutionMonoCache(); // 清理当前执行上下文的 Mono 缓存
                    });
        });
    }

    /**
     * 获取或创建指定节点实例的执行 Mono。
     * 使用 DagExecutionContext.getOrCreateNodeMono 确保为每个节点实例只创建一个 Mono。
     * 使用 Mono.cache() 确保该 Mono 的实际执行逻辑（包括依赖等待和节点执行）只运行一次。
     */
    private Mono<NodeResult<C, ?>> getNodeMono(
            String instanceName,
            DagExecutionContext<C> executionContext) { // 接收 DagExecutionContext

        // 使用 executionContext 的方法来获取或创建 Mono
        return executionContext.getOrCreateNodeMono(instanceName, () ->
                // Mono.defer: 延迟执行，直到有订阅发生。
                Mono.defer(() -> {
                            log.trace("[RequestId: {}][DAG: '{}'] Subscription received for node '{}'. Evaluating dependencies and execution status.",
                                    executionContext.getRequestId(), executionContext.getDagName(), instanceName);

                            // --- 1. 检查是否可以提前终止 (FAIL_FAST) ---
                            if (executionContext.getErrorStrategy() == ErrorHandlingStrategy.FAIL_FAST && executionContext.isFailFastActive()) {
                                log.debug("[RequestId: {}][DAG: '{}'] FAIL_FAST active, skipping node '{}' (cached check).",
                                        executionContext.getRequestId(), executionContext.getDagName(), instanceName);
                                return createSkippedResultMono(instanceName, executionContext);
                            }

                            // --- 2. 处理依赖 ---
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
                                            // 递归调用，传递相同的 executionContext
                                            return getNodeMono(upstreamName, executionContext);
                                        })
                                        .collect(Collectors.toList());
                            }

                            // --- 3. 等待依赖并执行 ---
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

                                        // --- 4. 评估条件并准备输入 ---
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

                                        // --- 5. 执行或跳过 ---
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
                                            // 调用 NodeExecutor, 传递 requestId 从 executionContext
                                            executionOrSkipMono = nodeExecutor.executeNode(nodeDef, executionContext.getInitialContext(), inputAccessor, executionContext.getRequestId());
                                        } else {
                                            log.debug("[RequestId: {}][DAG: '{}'] Conditions not met or critical dependencies failed/skipped. Skipping node '{}'.",
                                                    executionContext.getRequestId(), executionContext.getDagName(), instanceName);
                                            executionOrSkipMono = createSkippedResultMono(instanceName, executionContext);
                                        }

                                        // --- 6. 处理结果/错误 ---
                                        return executionOrSkipMono
                                                .doOnNext(result -> {
                                                    // 使用 executionContext.recordCompletedResult
                                                    if (executionContext.recordCompletedResult(instanceName, result)) {
                                                        if (result.isFailure()) {
                                                            handleFailureInternal(instanceName, result.getError().orElse(new RuntimeException("Unknown failure")), executionContext);
                                                        }
                                                    }
                                                    // 日志已在 recordCompletedResult 中处理
                                                })
                                                .doOnError(error -> {
                                                    log.error("[RequestId: {}][DAG: '{}'] Unexpected error during execution/skip processing for node '{}'. Recording failure.",
                                                            executionContext.getRequestId(), executionContext.getDagName(), instanceName, error);
                                                    handleFailure(instanceName, error, executionContext); // 传递 executionContext
                                                });
                                    })); // end Mono.when.then
                        }) // end Mono.defer
                        .cache() // 仍然缓存 Mono 的结果
        ); // end executionContext.getOrCreateNodeMono
    }


    private Mono<NodeResult<C, ?>> createSkippedResultMono(String instanceName, DagExecutionContext<C> context) {
        NodeResult<C, ?> skippedResult = NodeResult.skipped();
        // 使用 context.recordCompletedResult 来记录并获取日志
        context.recordCompletedResult(instanceName, skippedResult);
        // overallSuccess 不在此处修改，跳过不代表整体失败
        return Mono.just(skippedResult);
    }

    private void handleFailure(String instanceName, Throwable error, DagExecutionContext<C> context) {
        NodeResult<C, ?> failureResult = NodeResult.failure(error);
        // 使用 context.recordCompletedResult 来记录
        if (context.recordCompletedResult(instanceName, failureResult)) {
            // 如果是新记录的失败，则调用 internal 处理
            handleFailureInternal(instanceName, error, context);
        } else {
            // 如果已记录，仅记录警告 (已在 recordCompletedResult 中处理)
            log.warn("[RequestId: {}][DAG: '{}'] Node '{}' result ALREADY recorded when trying to mark as FAILURE. Error: {}.",
                    context.getRequestId(), context.getDagName(), instanceName, error.getMessage());
        }
    }

    private void handleFailureInternal(String instanceName, Throwable error, DagExecutionContext<C> context) {
        context.setOverallFailure(); // 标记整体失败
        if (context.getErrorStrategy() == ErrorHandlingStrategy.FAIL_FAST) {
            // 使用 context.triggerFailFast()
            if (context.triggerFailFast()) {
                // 日志已在 triggerFailFast 中处理
                log.debug("[RequestId: {}][DAG: '{}'] Node '{}' failed. FAIL_FAST strategy activated. Error: {}",
                        context.getRequestId(), context.getDagName(), instanceName, error.getMessage());
            } else {
                log.debug("[RequestId: {}][DAG: '{}'] Node '{}' failed, but FAIL_FAST was already active. Error: {}",
                        context.getRequestId(), context.getDagName(), instanceName, error.getMessage());
            }
        } else { // CONTINUE_ON_FAILURE
            log.debug("[RequestId: {}][DAG: '{}'] Node '{}' failed (CONTINUE_ON_FAILURE strategy). Execution continues. Error: {}",
                    context.getRequestId(), context.getDagName(), instanceName, error.getMessage());
        }
    }

    private void logCompletion(DagExecutionContext<C> context) {
        String finalStatus;
        if (context.getErrorStrategy() == ErrorHandlingStrategy.FAIL_FAST) {
            finalStatus = context.isFailFastActive() ? "FAILED (FAIL_FAST)" : (context.isOverallSuccess() ? "SUCCESS" : "FAILED");
        } else { // CONTINUE_ON_FAILURE
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
}