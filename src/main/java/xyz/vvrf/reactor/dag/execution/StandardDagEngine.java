// file: execution/StandardDagEngine.java
package xyz.vvrf.reactor.dag.execution;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.registry.NodeRegistry; // 需要 NodeRegistry

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * DagEngine 的标准实现。
 * 负责根据 DAG 定义编排节点执行，处理依赖关系、条件边和错误策略。
 *
 * @param <C> 上下文类型
 */
@Slf4j
public class StandardDagEngine<C> implements DagEngine<C> {

    private final NodeRegistry<C> nodeRegistry; // 需要特定上下文的注册表
    private final NodeExecutor<C> nodeExecutor; // 需要特定上下文的执行器
    private final int concurrencyLevel; // 引擎级别的并发控制（如果需要）

    /**
     * 创建 StandardDagEngine 实例。
     *
     * @param nodeRegistry     用于获取节点元数据（例如 InputSlot 定义）的、特定于上下文 C 的 NodeRegistry。
     * @param nodeExecutor     用于执行单个节点的、特定于上下文 C 的 NodeExecutor。
     * @param concurrencyLevel 用于控制 DAG 执行中潜在并发操作的级别（例如，如果使用 flatMap）。
     *                         注意：当前实现主要依赖 Reactor 的 Mono.when，并发性由其调度器管理。
     *                         此参数保留用于未来可能的并发策略调整。
     */
    public StandardDagEngine(NodeRegistry<C> nodeRegistry, NodeExecutor<C> nodeExecutor, int concurrencyLevel) {
        this.nodeRegistry = Objects.requireNonNull(nodeRegistry, "NodeRegistry cannot be null");
        this.nodeExecutor = Objects.requireNonNull(nodeExecutor, "NodeExecutor cannot be null");
        if (concurrencyLevel <= 0) {
            throw new IllegalArgumentException("Concurrency level must be positive.");
        }
        this.concurrencyLevel = concurrencyLevel; // 存储，但当前实现未使用
        log.info("StandardDagEngine for context '{}' initialized. Executor: {}, Concurrency Level (config): {}",
                nodeRegistry.getContextType().getSimpleName(),
                nodeExecutor.getClass().getSimpleName(),
                this.concurrencyLevel);
    }

    @Override
    public Flux<Event<?>> execute(C initialContext, DagDefinition<C> dagDefinition, String requestId) {
        // 确保 requestId 非空
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

        // --- 执行状态 ---
        final Map<String, Mono<NodeResult<C, ?>>> nodeExecutionMonos = new ConcurrentHashMap<>(); // 缓存节点执行 Mono
        final Map<String, NodeResult<C, ?>> completedResults = new ConcurrentHashMap<>(); // 存储最终结果
        final AtomicBoolean failFastTriggered = new AtomicBoolean(false); // FAIL_FAST 模式触发标志
        final AtomicBoolean overallSuccess = new AtomicBoolean(true); // 整体执行是否成功
        final AtomicInteger completedNodeCounter = new AtomicInteger(0); // 已完成（任何状态）节点计数器
        final ConditionInputAccessor<C> conditionAccessor = createConditionInputAccessor(completedResults); // 条件评估访问器

        // --- 执行流程 ---
        return Flux.defer(() -> {
            // 为图中 *所有* 节点获取执行 Mono (通过递归的 getNodeMono)
            List<Mono<Void>> executionFlows = allNodeNames.stream()
                    .map(nodeName -> getNodeMono( // 为每个节点获取其 Mono
                            nodeName, initialContext, dagDefinition, nodeExecutionMonos, completedResults,
                            failFastTriggered, overallSuccess, completedNodeCounter, totalNodes,
                            conditionAccessor, actualRequestId, dagName) // 传递 dagName
                            .then() // 只需要完成信号，忽略结果本身
                    )
                    .collect(Collectors.toList());

            // 使用 Mono.when 等待 *所有* 节点的完成信号
            // thenMany 在所有 Mono 完成后执行后续操作
            return Mono.when(executionFlows)
                    .thenMany(Flux.defer(() -> // 在所有节点完成后，从结果中提取并发射事件
                            Flux.fromIterable(completedResults.values())
                                    .filter(NodeResult::isSuccess) // 只关心成功节点的结果
                                    .flatMap(NodeResult::getEvents) // 提取事件流
                    ))
                    .doOnComplete(() -> logCompletion(actualRequestId, dagName, contextTypeName, errorStrategy, failFastTriggered.get(), overallSuccess.get(), completedNodeCounter.get(), totalNodes))
                    .doOnError(e -> {
                        // 这个 onError 通常捕获 Mono.when 或 thenMany 阶段的意外错误，而不是节点执行错误
                        log.error("[RequestId: {}][DAG: '{}'] Execution failed with unexpected error in main stream: {}", actualRequestId, dagName, e.getMessage(), e);
                        overallSuccess.set(false); // 标记整体失败
                        logCompletion(actualRequestId, dagName, contextTypeName, errorStrategy, failFastTriggered.get(), overallSuccess.get(), completedNodeCounter.get(), totalNodes);
                    })
                    .doFinally(signal -> {
                        log.debug("[RequestId: {}][DAG: '{}'] Execution finished (Signal: {}). Cleaning up resources.", actualRequestId, dagName, signal);
                        // 清理缓存或其他资源（如果需要）
                        nodeExecutionMonos.clear();
                        // completedResults 保留用于调试或后续分析，不清空
                    });
        });
    }

    /**
     * 获取或创建指定节点实例的执行 Mono。
     * 这是核心的递归方法，处理依赖、条件和执行。
     * 使用 computeIfAbsent 避免重复创建 Mono。
     */
    private Mono<NodeResult<C, ?>> getNodeMono(
            String instanceName,
            C context,
            DagDefinition<C> dagDefinition,
            Map<String, Mono<NodeResult<C, ?>>> nodeExecutionMonos, // Mono 缓存
            Map<String, NodeResult<C, ?>> completedResults,      // 结果存储
            AtomicBoolean failFastTriggered,
            AtomicBoolean overallSuccess,
            AtomicInteger completedNodeCounter,
            int totalNodes,
            ConditionInputAccessor<C> conditionAccessor,
            String requestId,
            String dagName) { // 接收 dagName

        // computeIfAbsent 确保每个节点只创建一个 Mono
        return nodeExecutionMonos.computeIfAbsent(instanceName, key ->
                Mono.defer(() -> { // 使用 defer 确保懒执行
                    log.trace("[RequestId: {}][DAG: '{}'] Evaluating node '{}' execution.", requestId, dagName, instanceName);

                    // --- 1. 检查是否可以提前终止 (FAIL_FAST) ---
                    if (dagDefinition.getErrorHandlingStrategy() == ErrorHandlingStrategy.FAIL_FAST && failFastTriggered.get()) {
                        log.debug("[RequestId: {}][DAG: '{}'] FAIL_FAST active, skipping node '{}' (cached check).", requestId, dagName, instanceName);
                        // 返回一个代表跳过的 Mono，并记录状态
                        return createSkippedResultMono(dagDefinition, instanceName, completedResults, completedNodeCounter, overallSuccess, requestId, dagName);
                    }

                    // --- 2. 处理依赖 ---
                    List<EdgeDefinition<C>> incomingEdges = dagDefinition.getIncomingEdges(instanceName);
                    List<Mono<NodeResult<C, ?>>> dependencyMonos;

                    if (incomingEdges.isEmpty()) {
                        // 没有入边，没有依赖
                        dependencyMonos = Collections.emptyList();
                        log.trace("[RequestId: {}][DAG: '{}'] Node '{}' has no dependencies.", requestId, dagName, instanceName);
                    } else {
                        // 获取所有上游节点的 Mono (递归调用)
                        dependencyMonos = incomingEdges.stream()
                                .map(EdgeDefinition::getUpstreamInstanceName)
                                .distinct() // 每个上游节点只需要一个 Mono
                                .map(upstreamName -> {
                                    log.trace("[RequestId: {}][DAG: '{}'] Node '{}' depends on '{}'. Getting dependency Mono.", requestId, dagName, instanceName, upstreamName);
                                    return getNodeMono( // 递归调用
                                            upstreamName, context, dagDefinition, nodeExecutionMonos, completedResults,
                                            failFastTriggered, overallSuccess, completedNodeCounter, totalNodes,
                                            conditionAccessor, requestId, dagName);
                                })
                                .collect(Collectors.toList());
                    }

                    // --- 3. 等待依赖并执行 ---
                    // 使用 Mono.when 等待所有依赖完成
                    return Mono.when(dependencyMonos)
                            .then(Mono.defer(() -> { // 所有依赖完成后，执行此 Mono 的内容
                                log.trace("[RequestId: {}][DAG: '{}'] Dependencies for node '{}' met. Proceeding.", requestId, dagName, instanceName);

                                // 再次检查 FAIL_FAST (可能在等待依赖期间被触发)
                                if (dagDefinition.getErrorHandlingStrategy() == ErrorHandlingStrategy.FAIL_FAST && failFastTriggered.get()) {
                                    log.debug("[RequestId: {}][DAG: '{}'] FAIL_FAST active after dependencies resolved, skipping node '{}'.", requestId, dagName, instanceName);
                                    return createSkippedResultMono(dagDefinition, instanceName, completedResults, completedNodeCounter, overallSuccess, requestId, dagName);
                                }

                                // --- 4. 评估条件并准备输入 ---
                                List<EdgeDefinition<C>> activeEdges = new ArrayList<>(); // 存储激活的边
                                boolean canExecute = false; // 标记节点是否满足执行条件
                                NodeDefinition nodeDef = dagDefinition.getNodeDefinition(instanceName)
                                        .orElseThrow(() -> new IllegalStateException("Node definition disappeared for: " + instanceName)); // 理论上不应发生

                                if (incomingEdges.isEmpty()) {
                                    // 没有入边，是起始节点，可以直接执行
                                    canExecute = true;
                                    log.trace("[RequestId: {}][DAG: '{}'] Node '{}' is a source node, can execute.", requestId, dagName, instanceName);
                                } else {
                                    boolean upstreamErrorInFailFast = false; // 标记在 FAIL_FAST 模式下是否有上游失败
                                    boolean upstreamSkipped = false; // 标记是否有上游被跳过（可能影响执行）

                                    for (EdgeDefinition<C> edge : incomingEdges) {
                                        String upstreamName = edge.getUpstreamInstanceName();
                                        NodeResult<C, ?> upstreamResult = completedResults.get(upstreamName);

                                        if (upstreamResult == null) {
                                            // 这是一个严重错误，依赖应该已经完成
                                            String errorMsg = String.format("Consistency error: Upstream node '%s' result not found for edge to '%s', though dependencies should be met.", upstreamName, instanceName);
                                            log.error("[RequestId: {}][DAG: '{}'] {}", requestId, dagName, errorMsg);
                                            handleFailure(instanceName, new IllegalStateException(errorMsg), dagDefinition, completedResults, failFastTriggered, overallSuccess, completedNodeCounter, requestId, dagName);
                                            return Mono.just(completedResults.get(instanceName)); // 返回失败结果
                                        }

                                        log.trace("[RequestId: {}][DAG: '{}'] Checking edge {} -> {}. Upstream status: {}", requestId, dagName, upstreamName, instanceName, upstreamResult.getStatus());

                                        if (upstreamResult.isSuccess()) {
                                            // 上游成功，评估条件
                                            boolean conditionMet;
                                            try {
                                                conditionMet = edge.getCondition().evaluate(context, conditionAccessor);
                                                log.trace("[RequestId: {}][DAG: '{}'] Condition evaluation for edge {} -> {}: {}", requestId, dagName, upstreamName, instanceName, conditionMet);
                                            } catch (Exception e) {
                                                log.error("[RequestId: {}][DAG: '{}'] Condition evaluation failed for edge {} -> {}. Treating as node failure.",
                                                        requestId, dagName, upstreamName, instanceName, e);
                                                handleFailure(instanceName, e, dagDefinition, completedResults, failFastTriggered, overallSuccess, completedNodeCounter, requestId, dagName);
                                                return Mono.just(completedResults.get(instanceName)); // 返回失败结果
                                            }

                                            if (conditionMet) {
                                                activeEdges.add(edge);
                                                // 只要有一条边激活，理论上就可以执行（具体逻辑看节点内部）
                                                // 但我们通常需要所有必需的输入都满足
                                                canExecute = true; // 暂时标记为可执行，后续可能因其他原因跳过
                                            }
                                        } else if (upstreamResult.isFailure()) {
                                            // 上游失败
                                            if (dagDefinition.getErrorHandlingStrategy() == ErrorHandlingStrategy.FAIL_FAST) {
                                                upstreamErrorInFailFast = true;
                                                log.debug("[RequestId: {}][DAG: '{}'] Upstream node '{}' failed in FAIL_FAST mode. Node '{}' will be skipped.", requestId, dagName, upstreamName, instanceName);
                                                break; // FAIL_FAST 模式下，一个上游失败就足以跳过当前节点
                                            } else {
                                                // CONTINUE_ON_FAILURE 模式，记录失败，但继续检查其他边
                                                log.debug("[RequestId: {}][DAG: '{}'] Upstream node '{}' failed in CONTINUE_ON_FAILURE mode. Node '{}' might still execute if other inputs are available.", requestId, dagName, upstreamName, instanceName);
                                            }
                                        } else if (upstreamResult.isSkipped()) {
                                            // 上游被跳过
                                            upstreamSkipped = true;
                                            log.debug("[RequestId: {}][DAG: '{}'] Upstream node '{}' was skipped. Node '{}' might be skipped.", requestId, dagName, upstreamName, instanceName);
                                            // 如果一个必需的输入来自被跳过的节点，当前节点通常也应跳过
                                            // 这个逻辑通常在节点内部或通过 InputAccessor 判断，这里只做标记
                                        }
                                    } // end for each edge

                                    if (upstreamErrorInFailFast) {
                                        canExecute = false; // 在 FAIL_FAST 下，上游失败则不能执行
                                    }
                                    // TODO: 更精细的判断逻辑：如果所有必需的输入槽对应的上游都失败或跳过，即使 canExecute=true 也应该跳过？
                                    // 目前依赖节点内部逻辑通过 InputAccessor 处理这种情况。
                                }

                                // --- 5. 执行或跳过 ---
                                Mono<NodeResult<C, ?>> executionOrSkipMono;
                                if (canExecute) {
                                    log.debug("[RequestId: {}][DAG: '{}'] Conditions potentially met for node '{}'. Preparing to execute. Active Edges: {}",
                                            requestId, dagName, instanceName, activeEdges.stream().map(e -> e.getUpstreamInstanceName() + "->" + e.getDownstreamInstanceName()).collect(Collectors.joining(", ")));

                                    // 创建 InputAccessor
                                    InputAccessor<C> inputAccessor;
                                    try {
                                        // 需要 NodeRegistry 来获取节点的 InputSlot 定义
                                        NodeRegistry.NodeMetadata meta = nodeRegistry.getNodeMetadata(nodeDef.getNodeTypeId())
                                                .orElseThrow(() -> new IllegalStateException("Metadata not found for " + nodeDef.getNodeTypeId() + " in registry " + nodeRegistry.getClass().getSimpleName()));
                                        Set<InputSlot<?>> declaredInputSlots = meta.getInputSlots();

                                        // DefaultInputAccessor 需要所有入边、所有完成结果、激活的边
                                        inputAccessor = new DefaultInputAccessor<>(
                                                declaredInputSlots, incomingEdges, completedResults, activeEdges
                                        );
                                    } catch (Exception e) {
                                        log.error("[RequestId: {}][DAG: '{}'] Failed to create InputAccessor for node '{}'. Treating as node failure.",
                                                requestId, dagName, instanceName, e);
                                        handleFailure(instanceName, e, dagDefinition, completedResults, failFastTriggered, overallSuccess, completedNodeCounter, requestId, dagName);
                                        return Mono.just(completedResults.get(instanceName));
                                    }

                                    // 调用 NodeExecutor 执行节点
                                    executionOrSkipMono = nodeExecutor.executeNode(nodeDef, context, inputAccessor, requestId);

                                } else {
                                    // 条件不满足或上游失败/跳过导致无法执行
                                    log.debug("[RequestId: {}][DAG: '{}'] Conditions not met or critical dependencies failed/skipped. Skipping node '{}'.",
                                            requestId, dagName, instanceName);
                                    executionOrSkipMono = createSkippedResultMono(dagDefinition, instanceName, completedResults, completedNodeCounter, overallSuccess, requestId, dagName);
                                }

                                // --- 6. 处理结果/错误，并缓存 ---
                                // 使用 .cache() 避免重复执行，并确保结果对后续依赖者可用
                                return executionOrSkipMono
                                        .doOnNext(result -> {
                                            // 结果产生时，记录到 completedResults
                                            if (completedResults.putIfAbsent(instanceName, result) == null) {
                                                // 首次记录该节点结果
                                                int count = completedNodeCounter.incrementAndGet();
                                                log.debug("[RequestId: {}][DAG: '{}'] Node '{}' completed with status: {}. Progress: {}/{}",
                                                        requestId, dagName, instanceName, result.getStatus(), count, totalNodes);
                                                if (result.isFailure()) {
                                                    // 如果节点执行失败，处理失败逻辑（可能触发 FAIL_FAST）
                                                    handleFailureInternal(instanceName, result.getError().orElse(new RuntimeException("Unknown failure")),
                                                            dagDefinition, failFastTriggered, overallSuccess, requestId, dagName);
                                                } else if (!result.isSuccess()) {
                                                    // 例如 SKIPPED 状态
                                                    // overallSuccess 在 createSkippedResultMono 中可能已设为 false（如果跳过是问题）
                                                    // 这里通常不需要额外操作，除非需要特殊处理 SKIPPED
                                                }
                                            } else {
                                                // Mono 被重复订阅（理论上 cache() 会阻止逻辑重复执行，但这可能发生在并发场景或 Reactor 内部）
                                                log.trace("[RequestId: {}][DAG: '{}'] Node '{}' result already recorded, ignoring duplicate signal.",
                                                        requestId, dagName, instanceName);
                                            }
                                        })
                                        .doOnError(error -> {
                                            // 捕获 executeNode 或 createSkippedResultMono 内部未处理的错误
                                            log.error("[RequestId: {}][DAG: '{}'] Unexpected error during execution/skip processing for node '{}'.",
                                                    requestId, dagName, instanceName, error);
                                            // 确保记录为失败状态
                                            handleFailure(instanceName, error, dagDefinition, completedResults, failFastTriggered, overallSuccess, completedNodeCounter, requestId, dagName);
                                        })
                                        .cache(); // 缓存结果 Mono

                            }));
                })
        );
    }


    /**
     * 创建一个代表节点被跳过的 Mono<NodeResult>，并更新状态。
     */
    private Mono<NodeResult<C, ?>> createSkippedResultMono(DagDefinition<C> dagDefinition, String instanceName,
                                                           Map<String, NodeResult<C, ?>> completedResults,
                                                           AtomicInteger completedNodeCounter, AtomicBoolean overallSuccess,
                                                           String requestId, String dagName) {
        // 创建 SKIPPED 结果
        NodeResult<C, ?> skippedResult = NodeResult.skipped();
        // 尝试记录结果，只有第一次记录时才增加计数器
        if (completedResults.putIfAbsent(instanceName, skippedResult) == null) {
            int count = completedNodeCounter.incrementAndGet();
            log.debug("[RequestId: {}][DAG: '{}'] Node '{}' recorded as SKIPPED. Progress: {}/{}",
                    requestId, dagName, instanceName, count, dagDefinition.getAllNodeInstanceNames().size());
            // 根据策略，跳过可能不影响整体成功状态，也可能影响（例如，如果关键节点被跳过）
            // 默认情况下，我们假设跳过本身不代表失败，除非有特定逻辑要求
            // overallSuccess.set(false); // 取决于业务逻辑是否认为跳过是问题
        }
        // 返回包含跳过结果的 Mono
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
        // 创建 FAILURE 结果
        NodeResult<C, ?> failureResult = NodeResult.failure(error);
        // 尝试记录结果，只有第一次记录时才增加计数器和处理失败逻辑
        if (completedResults.putIfAbsent(instanceName, failureResult) == null) {
            int count = completedNodeCounter.incrementAndGet();
            log.debug("[RequestId: {}][DAG: '{}'] Node '{}' recorded as FAILURE. Progress: {}/{}",
                    requestId, dagName, instanceName, count, dagDefinition.getAllNodeInstanceNames().size());
            // 调用内部处理逻辑
            handleFailureInternal(instanceName, error, dagDefinition, failFastTriggered, overallSuccess, requestId, dagName);
        }
    }

    /**
     * 处理节点执行失败的核心逻辑（设置状态，可能触发 FAIL_FAST）。
     */
    private void handleFailureInternal(String instanceName, Throwable error, DagDefinition<C> dagDefinition,
                                       AtomicBoolean failFastTriggered, AtomicBoolean overallSuccess,
                                       String requestId, String dagName) {
        // 标记整体执行为失败
        overallSuccess.set(false);
        // 根据错误处理策略操作
        if (dagDefinition.getErrorHandlingStrategy() == ErrorHandlingStrategy.FAIL_FAST) {
            // 尝试原子性地设置 failFastTriggered 为 true
            if (failFastTriggered.compareAndSet(false, true)) {
                // 首次触发 FAIL_FAST
                log.warn("[RequestId: {}][DAG: '{}'] Node '{}' failed. Activating FAIL_FAST strategy. Error: {}",
                        requestId, dagName, instanceName, error.getMessage()); // 只记录消息，避免重复堆栈
            } else {
                // FAIL_FAST 已被其他节点触发
                log.debug("[RequestId: {}][DAG: '{}'] Node '{}' failed, but FAIL_FAST was already active. Error: {}",
                        requestId, dagName, instanceName, error.getMessage());
            }
        } else { // CONTINUE_ON_FAILURE
            log.warn("[RequestId: {}][DAG: '{}'] Node '{}' failed (CONTINUE_ON_FAILURE strategy). Error: {}",
                    requestId, dagName, instanceName, error.getMessage());
        }
    }

    /**
     * 创建 ConditionInputAccessor 的实现。
     * 这个访问器允许条件（Condition）查询图中任何已完成节点的状态和结果。
     */
    private ConditionInputAccessor<C> createConditionInputAccessor(Map<String, NodeResult<C, ?>> completedResults) {
        // 返回一个匿名内部类或 Lambda 实现
        return new ConditionInputAccessor<C>() {
            @Override
            public Optional<NodeResult.NodeStatus> getNodeStatus(String instanceName) {
                // 从 completedResults 获取结果，然后映射到状态
                return Optional.ofNullable(completedResults.get(instanceName)).map(NodeResult::getStatus);
            }

            @Override
            @SuppressWarnings("unchecked") // 需要抑制转换警告
            public <P> Optional<P> getNodePayload(String instanceName, Class<P> expectedType) {
                // 获取结果 -> 检查是否成功 -> 获取 Payload Optional -> 过滤类型 -> 转换类型
                return Optional.ofNullable(completedResults.get(instanceName))
                        .filter(NodeResult::isSuccess)       // 必须是成功状态
                        .flatMap(NodeResult::getPayload)     // 获取 Optional<Payload>
                        .filter(expectedType::isInstance) // 检查类型是否匹配
                        .map(p -> (P) p);                 // 安全地转换类型
            }

            @Override
            public Optional<Throwable> getNodeError(String instanceName) {
                // 获取结果 -> 检查是否失败 -> 获取 Error Optional
                return Optional.ofNullable(completedResults.get(instanceName))
                        .filter(NodeResult::isFailure)       // 必须是失败状态
                        .flatMap(NodeResult::getError);      // 获取 Optional<Throwable>
            }

            @Override
            public Set<String> getCompletedNodeNames() {
                // 返回当前已完成节点名称的不可变集合视图
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

