// file: execution/StandardDagEngine.java
package xyz.vvrf.reactor.dag.execution;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * DagEngine 的标准实现。
 * 负责根据 DAG 定义编排节点执行，处理依赖关系、条件边和错误策略。
 * 使用 computeIfAbsent 和 Mono.cache() 确保每个节点的执行逻辑最多只被调用一次。
 *
 * @param <C> 上下文类型
 */
@Slf4j
public class StandardDagEngine<C> implements DagEngine<C> {

    private final NodeRegistry<C> nodeRegistry;
    private final NodeExecutor<C> nodeExecutor;
    private final int concurrencyLevel; // 保留，当前未使用

    public StandardDagEngine(NodeRegistry<C> nodeRegistry, NodeExecutor<C> nodeExecutor, int concurrencyLevel) {
        this.nodeRegistry = Objects.requireNonNull(nodeRegistry, "NodeRegistry cannot be null");
        this.nodeExecutor = Objects.requireNonNull(nodeExecutor, "NodeExecutor cannot be null");
        if (concurrencyLevel <= 0) {
            throw new IllegalArgumentException("Concurrency level must be positive.");
        }
        this.concurrencyLevel = concurrencyLevel;
        log.info("StandardDagEngine for context '{}' initialized. Executor: {}, Concurrency Level (config): {}",
                nodeRegistry.getContextType().getSimpleName(),
                nodeExecutor.getClass().getSimpleName(),
                this.concurrencyLevel);
    }

    @Override
    public Flux<Event<?>> execute(C initialContext, DagDefinition<C> dagDefinition, String requestId) {
        final String actualRequestId = (requestId != null && !requestId.trim().isEmpty())
                ? requestId
                : "dag-req-" + UUID.randomUUID().toString().substring(0, 8);
        final String dagName = dagDefinition.getDagName();
        final ErrorHandlingStrategy errorStrategy = dagDefinition.getErrorHandlingStrategy();
        final Set<String> allNodeNames = dagDefinition.getAllNodeInstanceNames();
        final int totalNodes = allNodeNames.size();
        final String contextTypeName = dagDefinition.getContextType().getSimpleName();

        log.info("[RequestId: {}][DAG: '{}'][Context: {}] Starting execution (Strategy: {}, Nodes: {})",
                actualRequestId, dagName, contextTypeName, errorStrategy, totalNodes);

        if (totalNodes == 0) {
            log.info("[RequestId: {}][DAG: '{}'] DAG is empty, returning empty Flux.", actualRequestId, dagName);
            return Flux.empty();
        }

        final Map<String, Mono<NodeResult<C, ?>>> nodeExecutionMonos = new ConcurrentHashMap<>();
        final Map<String, NodeResult<C, ?>> completedResults = new ConcurrentHashMap<>();
        final AtomicBoolean failFastTriggered = new AtomicBoolean(false);
        final AtomicBoolean overallSuccess = new AtomicBoolean(true);
        final AtomicInteger completedNodeCounter = new AtomicInteger(0);

        return Flux.defer(() -> {
            log.debug("[RequestId: {}][DAG: '{}'] Initializing execution flows for all {} nodes.", actualRequestId, dagName, totalNodes);
            List<Mono<Void>> executionFlows = allNodeNames.stream()
                    .map(nodeName -> getNodeMono( // 递归获取或创建每个节点的 Mono
                            nodeName, initialContext, dagDefinition, nodeExecutionMonos, completedResults,
                            failFastTriggered, overallSuccess, completedNodeCounter, totalNodes,
                            actualRequestId, dagName)
                            .then() // 我们只需要完成信号，结果通过 completedResults 共享
                    )
                    .collect(Collectors.toList());

            // Mono.when 等待所有节点的 Mono 完成（无论成功、失败还是跳过）
            return Mono.when(executionFlows)
                    .doOnSubscribe(s -> log.debug("[RequestId: {}][DAG: '{}'] Mono.when subscribed, waiting for all node flows to complete.", actualRequestId, dagName))
                    .thenMany(Flux.defer(() -> { // 所有节点流程结束后执行
                        log.debug("[RequestId: {}][DAG: '{}'] All node flows completed. Extracting events from successful results.", actualRequestId, dagName);
                        return Flux.fromIterable(completedResults.values())
                                .filter(NodeResult::isSuccess)
                                .flatMap(NodeResult::getEvents);
                    }))
                    .doOnComplete(() -> logCompletion(actualRequestId, dagName, contextTypeName, errorStrategy, failFastTriggered.get(), overallSuccess.get(), completedNodeCounter.get(), totalNodes))
                    .doOnError(e -> {
                        log.error("[RequestId: {}][DAG: '{}'] Execution failed with unexpected error in main stream: {}", actualRequestId, dagName, e.getMessage(), e);
                        overallSuccess.set(false);
                        logCompletion(actualRequestId, dagName, contextTypeName, errorStrategy, failFastTriggered.get(), overallSuccess.get(), completedNodeCounter.get(), totalNodes);
                    })
                    .doFinally(signal -> {
                        log.debug("[RequestId: {}][DAG: '{}'] Execution finished (Signal: {}). Cleaning up execution mono cache.", actualRequestId, dagName, signal);
                        nodeExecutionMonos.clear(); // 清理 Mono 缓存
                        // completedResults 保留用于调试或后续分析
                    });
        });
    }

    /**
     * 获取或创建指定节点实例的执行 Mono。
     * 使用 computeIfAbsent 确保为每个节点实例只创建一个 Mono。
     * 使用 Mono.cache() 确保该 Mono 的实际执行逻辑（包括依赖等待和节点执行）只运行一次。
     * 后续对此 Mono 的订阅将收到缓存的结果。
     */
    private Mono<NodeResult<C, ?>> getNodeMono(
            String instanceName,
            C context,
            DagDefinition<C> dagDefinition,
            Map<String, Mono<NodeResult<C, ?>>> nodeExecutionMonos,
            Map<String, NodeResult<C, ?>> completedResults,
            AtomicBoolean failFastTriggered,
            AtomicBoolean overallSuccess,
            AtomicInteger completedNodeCounter,
            int totalNodes,
            String requestId,
            String dagName) {

        // computeIfAbsent: 原子性地获取或创建 Mono。lambda 只会为每个 instanceName 执行一次。
        return nodeExecutionMonos.computeIfAbsent(instanceName, key -> {
            log.debug("[RequestId: {}][DAG: '{}'] Creating execution Mono for node '{}' (this happens only once per node).", requestId, dagName, instanceName);
            // Mono.defer: 延迟执行，直到有订阅发生。
            return Mono.defer(() -> {
                        // 这个日志会在每次下游节点需要此节点结果时（即订阅发生时）打印，即使结果是从缓存返回的。
                        log.trace("[RequestId: {}][DAG: '{}'] Subscription received for node '{}'. Evaluating dependencies and execution status.", requestId, dagName, instanceName);

                        // --- 1. 检查是否可以提前终止 (FAIL_FAST) ---
                        if (dagDefinition.getErrorHandlingStrategy() == ErrorHandlingStrategy.FAIL_FAST && failFastTriggered.get()) {
                            log.debug("[RequestId: {}][DAG: '{}'] FAIL_FAST active, skipping node '{}' (cached check).", requestId, dagName, instanceName);
                            return createSkippedResultMono(dagDefinition, instanceName, completedResults, completedNodeCounter, overallSuccess, requestId, dagName);
                        }

                        // --- 2. 处理依赖 ---
                        List<EdgeDefinition<C>> incomingEdges = dagDefinition.getIncomingEdges(instanceName);
                        List<Mono<NodeResult<C, ?>>> dependencyMonos;

                        if (incomingEdges.isEmpty()) {
                            dependencyMonos = Collections.emptyList();
                            log.trace("[RequestId: {}][DAG: '{}'] Node '{}' has no dependencies.", requestId, dagName, instanceName);
                        } else {
                            dependencyMonos = incomingEdges.stream()
                                    .map(EdgeDefinition::getUpstreamInstanceName)
                                    .distinct()
                                    .map(upstreamName -> {
                                        log.trace("[RequestId: {}][DAG: '{}'] Node '{}' depends on '{}'. Getting/creating dependency Mono.", requestId, dagName, instanceName, upstreamName);
                                        // 递归调用，获取上游节点的 Mono (可能是新建的，也可能是从缓存获取的)
                                        return getNodeMono(
                                                upstreamName, context, dagDefinition, nodeExecutionMonos, completedResults,
                                                failFastTriggered, overallSuccess, completedNodeCounter, totalNodes,
                                                requestId, dagName);
                                    })
                                    .collect(Collectors.toList());
                        }

                        // --- 3. 等待依赖并执行 ---
                        // Mono.when: 等待所有依赖 Mono 完成。订阅这些依赖 Mono 时，如果它们已被缓存，将直接获得结果。
                        return Mono.when(dependencyMonos)
                                .doOnSubscribe(s -> log.trace("[RequestId: {}][DAG: '{}'] Subscribed to dependencies for node '{}'. Waiting...", requestId, dagName, instanceName))
                                .then(Mono.defer(() -> { // 所有依赖完成后执行
                                    log.trace("[RequestId: {}][DAG: '{}'] Dependencies for node '{}' met. Proceeding to evaluate conditions and execute.", requestId, dagName, instanceName);

                                    // 再次检查 FAIL_FAST (可能在等待依赖期间被触发)
                                    if (dagDefinition.getErrorHandlingStrategy() == ErrorHandlingStrategy.FAIL_FAST && failFastTriggered.get()) {
                                        log.debug("[RequestId: {}][DAG: '{}'] FAIL_FAST active after dependencies resolved, skipping node '{}'.", requestId, dagName, instanceName);
                                        return createSkippedResultMono(dagDefinition, instanceName, completedResults, completedNodeCounter, overallSuccess, requestId, dagName);
                                    }

                                    // --- 4. 评估条件并准备输入 ---
                                    List<EdgeDefinition<C>> activeEdges = new ArrayList<>();
                                    boolean canExecute = false;
                                    NodeDefinition nodeDef = dagDefinition.getNodeDefinition(instanceName)
                                            .orElseThrow(() -> new IllegalStateException("Node definition disappeared for: " + instanceName));

                                    if (incomingEdges.isEmpty()) {
                                        canExecute = true;
                                        log.trace("[RequestId: {}][DAG: '{}'] Node '{}' is a source node, can execute.", requestId, dagName, instanceName);
                                    } else {
                                        boolean upstreamErrorInFailFast = false;
                                        boolean upstreamSkipped = false;

                                        for (EdgeDefinition<C> edge : incomingEdges) {
                                            String upstreamName = edge.getUpstreamInstanceName();
                                            // 从 completedResults 获取已完成的上游结果 (Mono.when 保证了它们此时已完成)
                                            NodeResult<C, ?> upstreamResult = completedResults.get(upstreamName);

                                            if (upstreamResult == null) {
                                                String errorMsg = String.format("Consistency error: Upstream node '%s' result not found for edge to '%s', though dependencies should be met.", upstreamName, instanceName);
                                                log.error("[RequestId: {}][DAG: '{}'] {}", requestId, dagName, errorMsg);
                                                handleFailure(instanceName, new IllegalStateException(errorMsg), dagDefinition, completedResults, failFastTriggered, overallSuccess, completedNodeCounter, requestId, dagName);
                                                // 返回包含失败结果的 Mono，以便下游知道
                                                return Mono.just(completedResults.get(instanceName));
                                            }

                                            log.trace("[RequestId: {}][DAG: '{}'] Checking edge {} -> {}. Upstream status: {}", requestId, dagName, upstreamName, instanceName, upstreamResult.getStatus());

                                            if (upstreamResult.isSuccess()) {
                                                boolean conditionMet = false;
                                                ConditionBase<C> condition = edge.getCondition();
                                                String conditionTypeName = condition.getClass().getSimpleName(); // 用于日志

                                                try {
                                                    // 根据条件类型进行评估
                                                    if (condition instanceof DirectUpstreamCondition) {
                                                        @SuppressWarnings({"rawtypes", "unchecked"}) // 必须抑制原始类型和可能的未检查调用警告
                                                        DirectUpstreamCondition rawDirectCond = (DirectUpstreamCondition) condition;
                                                        conditionMet = rawDirectCond.evaluate(context, upstreamResult);
                                                        log.trace("[RequestId: {}][DAG: '{}'] Evaluated DirectUpstreamCondition for edge {} -> {}: {}", requestId, dagName, upstreamName, instanceName, conditionMet);

                                                    } else if (condition instanceof LocalInputCondition) {
                                                        LocalInputCondition<C> localCond = (LocalInputCondition<C>) condition;
                                                        // 创建 LocalInputAccessor (需要传入 dagDefinition 和下游节点名 instanceName)
                                                        LocalInputAccessor<C> localAccessor = new xyz.vvrf.reactor.dag.execution.DefaultLocalInputAccessor<>(completedResults, dagDefinition, instanceName);
                                                        conditionMet = localCond.evaluate(context, localAccessor);
                                                        log.trace("[RequestId: {}][DAG: '{}'] Evaluated LocalInputCondition for edge {} -> {}: {}", requestId, dagName, upstreamName, instanceName, conditionMet);

                                                    } else if (condition instanceof DeclaredDependencyCondition) {
                                                        DeclaredDependencyCondition<C> declaredCond = (DeclaredDependencyCondition<C>) condition;
                                                        Set<String> explicitDeps = declaredCond.getRequiredNodeDependencies();
                                                        Set<String> allowedNodes = new HashSet<>(explicitDeps);
                                                        allowedNodes.add(upstreamName); // 添加直接上游

                                                        // 创建受限的全局 Accessor (复用之前的 Restricted 实现)
                                                        ConditionInputAccessor<C> restrictedAccessor = new RestrictedConditionInputAccessor<>(
                                                                completedResults, allowedNodes, upstreamName);
                                                        conditionMet = declaredCond.evaluate(context, restrictedAccessor);
                                                        log.trace("[RequestId: {}][DAG: '{}'] Evaluated DeclaredDependencyCondition for edge {} -> {} (Allowed: {}): {}",
                                                                requestId, dagName, upstreamName, instanceName, allowedNodes, conditionMet);
                                                    } else {
                                                        // 未知条件类型，视为失败
                                                        log.error("[RequestId: {}][DAG: '{}'] Unknown condition type '{}' for edge {} -> {}. Treating as false.",
                                                                requestId, dagName, condition.getClass().getName(), upstreamName, instanceName);
                                                        conditionMet = false;
                                                        // 或者抛出异常？当前选择视为 false
                                                    }

                                                } catch (IllegalArgumentException accessError) {
                                                    // 捕获 RestrictedConditionInputAccessor 的访问错误
                                                    log.error("[RequestId: {}][DAG: '{}'] Condition ({}) evaluation for edge {} -> {} failed due to restricted access: {}. Treating as node failure.",
                                                            requestId, dagName, conditionTypeName, upstreamName, instanceName, accessError.getMessage());
                                                    handleFailure(instanceName, accessError, dagDefinition, completedResults, failFastTriggered, overallSuccess, completedNodeCounter, requestId, dagName);
                                                    return Mono.just(completedResults.get(instanceName));
                                                } catch (Exception e) {
                                                    // 捕获条件评估本身的其他异常
                                                    log.error("[RequestId: {}][DAG: '{}'] Condition ({}) evaluation failed unexpectedly for edge {} -> {}. Treating as node failure.",
                                                            requestId, dagName, conditionTypeName, upstreamName, instanceName, e);
                                                    handleFailure(instanceName, e, dagDefinition, completedResults, failFastTriggered, overallSuccess, completedNodeCounter, requestId, dagName);
                                                    return Mono.just(completedResults.get(instanceName));
                                                }

                                                if (conditionMet) {
                                                    activeEdges.add(edge);
                                                    canExecute = true; // 至少有一条激活路径
                                                }
                                            } else if (upstreamResult.isFailure()) {
                                                if (dagDefinition.getErrorHandlingStrategy() == ErrorHandlingStrategy.FAIL_FAST) {
                                                    upstreamErrorInFailFast = true;
                                                    log.debug("[RequestId: {}][DAG: '{}'] Upstream node '{}' failed in FAIL_FAST mode. Node '{}' will be skipped.", requestId, dagName, upstreamName, instanceName);
                                                    break;
                                                } else {
                                                    log.debug("[RequestId: {}][DAG: '{}'] Upstream node '{}' failed in CONTINUE_ON_FAILURE mode. Node '{}' might still execute if other inputs are available.", requestId, dagName, upstreamName, instanceName);
                                                }
                                            } else if (upstreamResult.isSkipped()) {
                                                upstreamSkipped = true;
                                                log.debug("[RequestId: {}][DAG: '{}'] Upstream node '{}' was skipped. Node '{}' might be skipped.", requestId, dagName, upstreamName, instanceName);
                                            }
                                        } // end for each edge

                                        if (upstreamErrorInFailFast) {
                                            canExecute = false;
                                        }
                                        // TODO: 考虑更复杂的跳过逻辑，例如如果所有必需输入都被跳过？
                                    }

                                    // --- 5. 执行或跳过 ---
                                    Mono<NodeResult<C, ?>> executionOrSkipMono;
                                    if (canExecute) {
                                        log.debug("[RequestId: {}][DAG: '{}'] Conditions met for node '{}'. Preparing to execute via NodeExecutor. Active Edges: {}",
                                                requestId, dagName, instanceName, activeEdges.stream().map(e -> e.getUpstreamInstanceName() + "->" + e.getDownstreamInstanceName()).collect(Collectors.joining(", ")));

                                        InputAccessor<C> inputAccessor;
                                        try {
                                            NodeRegistry.NodeMetadata meta = nodeRegistry.getNodeMetadata(nodeDef.getNodeTypeId())
                                                    .orElseThrow(() -> new IllegalStateException("Metadata not found for " + nodeDef.getNodeTypeId() + " in registry " + nodeRegistry.getClass().getSimpleName()));
                                            Set<InputSlot<?>> declaredInputSlots = meta.getInputSlots();
                                            inputAccessor = new DefaultInputAccessor<>(declaredInputSlots, incomingEdges, completedResults, activeEdges);
                                        } catch (Exception e) {
                                            log.error("[RequestId: {}][DAG: '{}'] Failed to create InputAccessor for node '{}'. Treating as node failure.",
                                                    requestId, dagName, instanceName, e);
                                            handleFailure(instanceName, e, dagDefinition, completedResults, failFastTriggered, overallSuccess, completedNodeCounter, requestId, dagName);
                                            return Mono.just(completedResults.get(instanceName));
                                        }

                                        // 调用 NodeExecutor 执行节点，返回代表执行结果的 Mono
                                        executionOrSkipMono = nodeExecutor.executeNode(nodeDef, context, inputAccessor, requestId);

                                    } else {
                                        log.debug("[RequestId: {}][DAG: '{}'] Conditions not met or critical dependencies failed/skipped. Skipping node '{}'.",
                                                requestId, dagName, instanceName);
                                        executionOrSkipMono = createSkippedResultMono(dagDefinition, instanceName, completedResults, completedNodeCounter, overallSuccess, requestId, dagName);
                                    }

                                    // --- 6. 处理结果/错误，并准备缓存 ---
                                    // doOnNext/doOnError 在 Mono 成功/失败时执行副作用（记录结果）
                                    // 这些副作用只会在 Mono 第一次执行时发生，因为后续将由 cache() 处理。
                                    return executionOrSkipMono
                                            .doOnNext(result -> {
                                                // 结果产生时，原子性地记录到 completedResults
                                                if (completedResults.putIfAbsent(instanceName, result) == null) {
                                                    int count = completedNodeCounter.incrementAndGet();
                                                    log.debug("[RequestId: {}][DAG: '{}'] Node '{}' completed with status: {}. Recorded result. Progress: {}/{}",
                                                            requestId, dagName, instanceName, result.getStatus(), count, totalNodes);
                                                    if (result.isFailure()) {
                                                        handleFailureInternal(instanceName, result.getError().orElse(new RuntimeException("Unknown failure")),
                                                                dagDefinition, failFastTriggered, overallSuccess, requestId, dagName);
                                                    }
                                                    // Skipped 状态在 createSkippedResultMono 中处理
                                                } else {
                                                    // Mono 被重复订阅（理论上 cache() 会阻止逻辑重复执行，但此日志可能指示并发问题或 Reactor 内部行为）
                                                    log.warn("[RequestId: {}][DAG: '{}'] Node '{}' result ALREADY recorded, but doOnNext triggered again? Status: {}. This might indicate an issue.",
                                                            requestId, dagName, instanceName, result.getStatus());
                                                }
                                            })
                                            .doOnError(error -> {
                                                // 捕获 executeNode 或 createSkippedResultMono 内部未处理的错误
                                                log.error("[RequestId: {}][DAG: '{}'] Unexpected error during execution/skip processing for node '{}'. Recording failure.",
                                                        requestId, dagName, instanceName, error);
                                                // 确保记录为失败状态 (handleFailure 会处理 putIfAbsent)
                                                handleFailure(instanceName, error, dagDefinition, completedResults, failFastTriggered, overallSuccess, completedNodeCounter, requestId, dagName);
                                            });
                                    // 注意：cache() 操作符在 computeIfAbsent 的 lambda 返回之前隐式应用 (见下方 .cache())
                                })); // end Mono.when.then
                    }) // end Mono.defer
                    // .cache(): 核心！确保上面 defer 返回的整个 Mono 链 (包括依赖等待、执行、结果记录)
                    // 只会被实际订阅和执行一次。后续的订阅者将直接收到缓存的最终结果 (onNext/onError/onComplete)。
                    // 这可以防止节点的重复执行。
                    .cache();
        }); // end computeIfAbsent
    }


    /**
     * 创建一个代表节点被跳过的 Mono<NodeResult>，并更新状态。
     */
    private Mono<NodeResult<C, ?>> createSkippedResultMono(DagDefinition<C> dagDefinition, String instanceName,
                                                           Map<String, NodeResult<C, ?>> completedResults,
                                                           AtomicInteger completedNodeCounter, AtomicBoolean overallSuccess,
                                                           String requestId, String dagName) {
        NodeResult<C, ?> skippedResult = NodeResult.skipped();
        if (completedResults.putIfAbsent(instanceName, skippedResult) == null) {
            int count = completedNodeCounter.incrementAndGet();
            log.debug("[RequestId: {}][DAG: '{}'] Node '{}' recorded as SKIPPED. Progress: {}/{}",
                    requestId, dagName, instanceName, count, dagDefinition.getAllNodeInstanceNames().size());
            // 跳过本身通常不标记整体失败，除非业务逻辑需要
        } else {
            log.warn("[RequestId: {}][DAG: '{}'] Node '{}' result ALREADY recorded when trying to mark as SKIPPED. This might indicate an issue.",
                    requestId, dagName, instanceName);
        }
        return Mono.just(skippedResult);
    }

    /**
     * 处理节点执行失败（外部调用点，确保结果被记录）。
     */
    private void handleFailure(String instanceName, Throwable error, DagDefinition<C> dagDefinition,
                               Map<String, NodeResult<C, ?>> completedResults,
                               AtomicBoolean failFastTriggered, AtomicBoolean overallSuccess,
                               AtomicInteger completedNodeCounter,
                               String requestId, String dagName) {
        NodeResult<C, ?> failureResult = NodeResult.failure(error);
        if (completedResults.putIfAbsent(instanceName, failureResult) == null) {
            int count = completedNodeCounter.incrementAndGet();
            // 使用 warn 级别记录失败事件
            log.warn("[RequestId: {}][DAG: '{}'] Node '{}' recorded as FAILURE. Progress: {}/{}. Error: {}",
                    requestId, dagName, instanceName, count, dagDefinition.getAllNodeInstanceNames().size(), error.getMessage());
            handleFailureInternal(instanceName, error, dagDefinition, failFastTriggered, overallSuccess, requestId, dagName);
        } else {
            log.warn("[RequestId: {}][DAG: '{}'] Node '{}' result ALREADY recorded when trying to mark as FAILURE. Error: {}. This might indicate an issue.",
                    requestId, dagName, instanceName, error.getMessage());
        }
    }

    /**
     * 处理节点执行失败的核心逻辑（设置状态，可能触发 FAIL_FAST）。
     */
    private void handleFailureInternal(String instanceName, Throwable error, DagDefinition<C> dagDefinition,
                                       AtomicBoolean failFastTriggered, AtomicBoolean overallSuccess,
                                       String requestId, String dagName) {
        overallSuccess.set(false); // 任何失败都会导致整体不成功
        if (dagDefinition.getErrorHandlingStrategy() == ErrorHandlingStrategy.FAIL_FAST) {
            if (failFastTriggered.compareAndSet(false, true)) {
                log.warn("[RequestId: {}][DAG: '{}'] Node '{}' failed. Activating FAIL_FAST strategy. Error: {}",
                        requestId, dagName, instanceName, error.getMessage()); // 只记录消息避免堆栈泛滥
            } else {
                log.debug("[RequestId: {}][DAG: '{}'] Node '{}' failed, but FAIL_FAST was already active. Error: {}",
                        requestId, dagName, instanceName, error.getMessage());
            }
        } else { // CONTINUE_ON_FAILURE
            // 失败已在 handleFailure 中以 warn 级别记录，这里不再重复记录
            log.debug("[RequestId: {}][DAG: '{}'] Node '{}' failed (CONTINUE_ON_FAILURE strategy). Execution continues.",
                    requestId, dagName, instanceName);
        }
    }

    /**
     * 创建 ConditionInputAccessor 的实现。
     */
    private ConditionInputAccessor<C> createConditionInputAccessor(Map<String, NodeResult<C, ?>> completedResults) {
        // 实现保持不变
        return new ConditionInputAccessor<C>() {
            @Override
            public Optional<NodeResult.NodeStatus> getNodeStatus(String instanceName) {
                return Optional.ofNullable(completedResults.get(instanceName)).map(NodeResult::getStatus);
            }

            @Override
            @SuppressWarnings("unchecked")
            public <P> Optional<P> getNodePayload(String instanceName, Class<P> expectedType) {
                return Optional.ofNullable(completedResults.get(instanceName))
                        .filter(NodeResult::isSuccess)
                        .flatMap(NodeResult::getPayload)
                        .filter(expectedType::isInstance)
                        .map(p -> (P) p);
            }

            @Override
            public Optional<Throwable> getNodeError(String instanceName) {
                return Optional.ofNullable(completedResults.get(instanceName))
                        .filter(NodeResult::isFailure)
                        .flatMap(NodeResult::getError);
            }

            @Override
            public Set<String> getCompletedNodeNames() {
                return Collections.unmodifiableSet(new HashSet<>(completedResults.keySet()));
            }
        };
    }

    /**
     * 记录 DAG 执行完成时的最终状态。
     */
    private void logCompletion(String requestId, String dagName, String contextTypeName, ErrorHandlingStrategy strategy,
                               boolean failFastTriggered, boolean overallSuccess, int completedCount, int totalNodes) {
        String finalStatus;
        if (strategy == ErrorHandlingStrategy.FAIL_FAST) {
            // FAIL_FAST 模式下，只要触发了就认为是 FAILED
            finalStatus = failFastTriggered ? "FAILED (FAIL_FAST)" : "SUCCESS";
        } else { // CONTINUE_ON_FAILURE
            // CONTINUE 模式下，根据 overallSuccess 判断
            finalStatus = overallSuccess ? "SUCCESS" : "COMPLETED_WITH_FAILURES";
        }
        boolean allAccountedFor = (completedCount == totalNodes);
        log.info("[RequestId: {}][DAG: '{}'][Context: {}] Execution finished. Final Status: {}. Completed nodes accounted for: {}/{}. All nodes processed: {}",
                requestId, dagName, contextTypeName, finalStatus, completedCount, totalNodes, allAccountedFor);
        if (!allAccountedFor) {
            // 如果完成计数与总节点数不匹配，可能意味着逻辑错误或并发问题
            log.warn("[RequestId: {}][DAG: '{}'] Potential issue: Not all nodes ({}) were accounted for upon completion ({}). This might indicate an issue in the execution logic or graph structure.",
                    requestId, dagName, totalNodes, completedCount);
        }
    }
}
