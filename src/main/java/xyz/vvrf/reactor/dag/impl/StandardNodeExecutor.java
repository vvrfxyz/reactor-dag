package xyz.vvrf.reactor.dag.impl;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.core.*;
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener; // 确保引入

import java.time.Instant;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * 标准节点执行器 - 负责执行单个节点并处理依赖关系和缓存。
 * 接收所有已完成节点的结果来创建 InputDependencyAccessor，
 * 并确保在执行前等待直接执行前驱完成。
 *
 * @author ruifeng.wen (Refactored by Devin based on requirements)
 */
@Slf4j
public class StandardNodeExecutor {

    private final Duration defaultNodeTimeout;
    private final Scheduler nodeExecutionScheduler;
    private final List<DagMonitorListener> monitorListeners; // 监听器列表

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
     * 获取或创建节点执行的 Mono<NodeResult>，支持请求级缓存 Mono 执行逻辑。
     * 此方法返回的 Mono 在订阅时会：
     * 1. 检查缓存中是否已有此节点的执行 Mono。
     * 2. 如果没有，则创建一个新的 Mono，该 Mono 会：
     *    a. 查找节点实例。
     *    b. 解析并等待所有 *直接执行* 前驱完成（利用缓存）。
     *    c. 基于传入的 *所有已完成* 节点的结果 (`allCompletedResults`) 创建 InputDependencyAccessor。
     *    d. 调用节点的 shouldExecute 和 execute 方法。
     *    e. 处理超时、重试和错误。
     * 3. 将新创建的 Mono 放入缓存（通过 .cache() 包装）。
     * 4. 返回缓存的或新创建的 Mono。
     *
     * @param <C>                 上下文类型
     * @param nodeName            节点名称
     * @param context             当前上下文
     * @param monoCache           请求级缓存 (Key: String, Value: Mono<? extends NodeResult<C, ?, ?>>)，用于缓存执行 Mono
     * @param allCompletedResults 当前执行中所有已完成节点的结果映射 (用于创建 Accessor)
     * @param dagDefinition       DAG 定义
     * @param requestId           请求 ID，用于日志
     * @return 返回一个 Mono<NodeResult<C, ?, ?>>，其具体的 P 和 T 类型由节点自身决定。
     */
    public <C> Mono<NodeResult<C, ?, ?>> getNodeExecutionMono(
            final String nodeName,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> monoCache,
            final Map<String, NodeResult<C, ?, ?>> allCompletedResults, // 接收所有结果
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String cacheKey = nodeName; // 使用 nodeName 作为缓存 Key
        final String dagName = dagDefinition.getDagName();

        // 尝试从缓存获取
        Mono<? extends NodeResult<C, ?, ?>> cachedMono = monoCache.getIfPresent(cacheKey);
        if (cachedMono != null) {
            log.trace("[RequestId: {}] DAG '{}': 缓存命中节点 '{}' 的执行 Mono",
                    requestId, dagName, nodeName);
            // 直接返回缓存的 Mono，类型是通配符
            // 使用 Mono.from() 确保返回的是 Mono 类型，即使缓存中的实际类型是子类 Mono
            return Mono.from(cachedMono);
        } else {
            log.debug("[RequestId: {}] DAG '{}': 缓存未命中，创建节点 '{}' 执行 Mono",
                    requestId, dagName, nodeName);

            // 创建新的执行 Mono，传递 allCompletedResults
            // 使用 Mono.defer 确保每次订阅时都执行查找和依赖解析逻辑
            Mono<NodeResult<C, ?, ?>> newMono = Mono.defer(() ->
                    createNodeExecutionMonoInternal(
                            nodeName, context, monoCache, allCompletedResults, dagDefinition, requestId
                    )
            );

            // 使用 .cache() 缓存结果，并放入缓存
            // .cache() 确保底层逻辑只执行一次，后续订阅者直接获得缓存结果
            Mono<NodeResult<C, ?, ?>> monoToCache = newMono.cache();
            monoCache.put(cacheKey, monoToCache); // 放入缓存

            // 返回这个会被缓存的 Mono
            return monoToCache;
        }
    }

    /**
     * 创建节点执行的 Mono (内部实现)。
     * 这个 Mono 在订阅时会查找节点、解析执行依赖、创建Accessor、执行节点逻辑。
     */
    private <C> Mono<NodeResult<C, ?, ?>> createNodeExecutionMonoInternal(
            final String nodeName,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> monoCache,
            final Map<String, NodeResult<C, ?, ?>> allCompletedResults, // 接收所有结果
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();
        Instant nodeProcessingStartTime = Instant.now(); // 记录节点处理开始时间 (包括依赖解析)

        // 使用 Mono.defer 包装查找逻辑，以便错误能被 Reactor 处理
        return Mono.defer(() -> {
            // 1. 获取节点实例 (不关心具体类型 P, T)
            final DagNode<C, ?, ?> node;
            try {
                node = dagDefinition.getNodeAnyType(nodeName)
                        .orElseThrow(() -> new NodeConfigurationException( // 直接抛出配置异常
                                String.format("Node '%s' not found in DAG definition.", nodeName), null, nodeName, dagName));
            } catch (NodeConfigurationException e) {
                // 如果 orElseThrow 抛出了我们的特定异常
                log.error("[RequestId: {}] DAG '{}': 配置错误 - 节点 '{}' 未找到。", requestId, dagName, nodeName, e);
                // 返回一个立即失败的 Mono，携带配置错误
                return Mono.error(e);
            } catch (Exception e) {
                // 处理查找过程中可能出现的其他意外异常
                log.error("[RequestId: {}] DAG '{}': 查找节点 '{}' 时发生意外错误。", requestId, dagName, nodeName, e);
                // 将其他异常也包装成配置异常返回
                return Mono.error(new NodeConfigurationException(
                        String.format("Unexpected error finding node '%s': %s", nodeName, e.getMessage()), e, nodeName, dagName));
            }

            // --- 后续逻辑在节点找到后执行 ---

            // 通知监听器节点开始处理 (包括等待依赖)
            safeNotifyListeners(l -> l.onNodeStart(requestId, dagName, nodeName, node));

            // 2. 解析并等待 *直接执行* 前驱完成 (为了保证拓扑顺序)
            Mono<Void> predecessorsCompletionMono = resolveExecutionPredecessors(
                    nodeName, context, monoCache, allCompletedResults, dagDefinition, requestId);

            // 3. 在所有执行前驱完成后，执行当前节点逻辑
            return predecessorsCompletionMono
                    .then(Mono.deferContextual(contextView -> { // 使用 deferContextual 传递计时信息
                        // ... (创建 Accessor, shouldExecute, executeNodeLogic 等逻辑不变) ...
                        // 4. 创建 InputDependencyAccessor
                        Map<InputRequirement<?>, String> nodeInputMappings = dagDefinition.getInputMappingForNode(nodeName);
                        InputDependencyAccessor<C> accessor = new DefaultInputDependencyAccessor<>(
                                allCompletedResults,
                                nodeInputMappings,
                                nodeName,
                                dagName);

                        // 5. 确定超时
                        Duration timeout = determineNodeTimeout(node);
                        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Impl: {}) 前驱完成，准备执行核心逻辑, 超时: {}",
                                requestId, dagName, nodeName, node.getClass().getSimpleName(), timeout);

                        // 6. 检查 shouldExecute
                        boolean shouldExec;
                        Instant logicStartTime = Instant.now();
                        try {
                            shouldExec = node.shouldExecute(context, accessor);
                        } catch (Exception e) {
                            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 的 shouldExecute 方法抛出异常，将视为不执行并产生错误结果。",
                                    requestId, dagName, nodeName, e);
                            Instant endTime = Instant.now();
                            Duration totalDuration = Duration.between(nodeProcessingStartTime, endTime);
                            Duration logicDuration = Duration.between(logicStartTime, endTime);
                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, e, node));
                            return Mono.just(createFailureResult(context, e, node)); // shouldExecute 运行时错误仍返回 Failure Result
                        }

                        // 7. 执行或跳过
                        if (shouldExec) {
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 条件满足，将执行。", requestId, dagName, nodeName);
                            Mono<NodeResult<C, ?, ?>> resultMono = executeNodeLogic(
                                    node, context, accessor, timeout, requestId, dagName, nodeProcessingStartTime, logicStartTime);
                            return resultMono;
                        } else {
                            log.info("[RequestId: {}] DAG '{}': 节点 '{}' 条件不满足，将被跳过。", requestId, dagName, nodeName);
                            Instant endTime = Instant.now();
                            Duration totalDuration = Duration.between(nodeProcessingStartTime, endTime);
                            safeNotifyListeners(l -> l.onNodeSkipped(requestId, dagName, nodeName, node));
                            NodeResult<C, ?, ?> skippedResult = createSkippedResult(context, node);
                            return Mono.just(skippedResult);
                        }
                    }))
                    // 捕获前驱解析或 shouldExecute/execute 过程中的错误
                    .onErrorResume(error -> {
                        // ... (onErrorResume 逻辑基本不变，处理运行时错误) ...
                        if (!(error instanceof NodeExecutionException || error instanceof NodeConfigurationException)) {
                            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 在执行前或前驱解析中失败: {}", requestId, dagName, nodeName, error.getMessage(), error);
                            Instant endTime = Instant.now();
                            Duration totalDuration = Duration.between(nodeProcessingStartTime, endTime);
                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, Duration.ZERO, error, node));
                            return Mono.just(createFailureResult(context, error, node));
                        } else {
                            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 捕获到已处理的错误类型: {}", requestId, dagName, nodeName, error.getClass().getSimpleName());
                            // 假设错误已被包装在 NodeResult 中或就是配置错误，创建/返回对应的 Failure Result
                            return Mono.just(createFailureResult(context, error, node));
                        }
                    });
        }); // 结束最外层的 Mono.defer
    }

    /**
     * 解析并等待节点的所有直接 *执行* 前驱完成。
     * 返回一个 Mono<Void>，该 Mono 在所有前驱的执行 Mono 完成后才完成。
     * 利用 getNodeExecutionMono 来获取前驱节点的 Mono，从而复用缓存逻辑。
     * **注意**: 此方法不收集前驱结果，只确保它们完成。
     */
    private <C> Mono<Void> resolveExecutionPredecessors(
            final String nodeName,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> monoCache,
            final Map<String, NodeResult<C, ?, ?>> allCompletedResults, // 仍然需要传递给 getNodeExecutionMono
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();
        Set<String> predecessorNames = dagDefinition.getExecutionPredecessors(nodeName);

        if (predecessorNames.isEmpty()) {
            log.trace("[RequestId: {}] DAG '{}': 节点 '{}' 没有执行前驱。", requestId, dagName, nodeName);
            return Mono.empty(); // 没有前驱，直接完成
        }

        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 正在等待 {} 个执行前驱完成: {}",
                requestId, dagName, nodeName, predecessorNames.size(), predecessorNames);

        // 获取所有前驱节点的执行 Mono (递归调用以利用缓存)
        List<Mono<?>> predecessorMonos = predecessorNames.stream()
                .map(predName -> getNodeExecutionMono(
                        predName,
                        context,
                        monoCache,
                        allCompletedResults, // 传递当前的累积结果
                        dagDefinition,
                        requestId
                ))
                .collect(Collectors.toList());

        // 使用 Mono.whenDelayError 等待所有前驱 Mono 完成 (即使有错误也等待所有完成)
        return Mono.whenDelayError(predecessorMonos)
                .doOnSuccess(v -> log.debug(
                        "[RequestId: {}] DAG '{}': 节点 '{}' 的所有 {} 个执行前驱已完成处理。",
                        requestId, dagName, nodeName, predecessorNames.size()))
                .doOnError(e -> log.warn( // Warn level, as individual errors are handled by the respective node's mono
                        "[RequestId: {}] DAG '{}': 节点 '{}' 等待前驱完成时检测到至少一个前驱失败: {}",
                        requestId, dagName, nodeName, e.getMessage())); // 错误会在上层处理或已被处理
    }


    /**
     * 内部方法：实际执行节点逻辑。
     * @param node 节点实例 (通配符类型)
     * @param accessor 依赖访问器 (基于所有已完成结果)
     * @param nodeProcessingStartTime 节点处理开始时间 (包括依赖解析)
     * @param logicStartTime 核心逻辑开始时间 (shouldExecute/execute)
     * @return Mono<NodeResult<C, ?, ?>>
     */
    private <C> Mono<NodeResult<C, ?, ?>> executeNodeLogic(
            DagNode<C, ?, ?> node, // 接收通配符类型
            C context,
            InputDependencyAccessor<C> accessor,
            Duration timeout,
            String requestId,
            String dagName,
            Instant nodeProcessingStartTime,
            Instant logicStartTime) {

        String nodeName = node.getName();
        Retry retrySpec = node.getRetrySpec();

        // 使用 Mono.defer 确保每次订阅（或重试）时都调用 execute
        return Mono.defer(() -> {
                    log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Impl: {}) 核心逻辑开始执行 (或重试)...",
                            requestId, dagName, nodeName, node.getClass().getSimpleName());

                    // 调用节点的 execute 方法，传入 accessor
                    // 需要进行类型转换，因为 node 是通配符类型
                    @SuppressWarnings("unchecked") // 强制转换，因为我们是从 DagDefinition 获取的实例
                    DagNode<C, Object, Object> typedNode = (DagNode<C, Object, Object>) node;

                    // 执行节点逻辑
                    Mono<NodeResult<C, Object, Object>> executionMono = typedNode.execute(context, accessor);

                    // 应用调度器、超时、日志、重试、错误处理
                    return executionMono
                            .subscribeOn(nodeExecutionScheduler) // 在指定调度器上执行
                            .timeout(timeout) // 应用超时
                            .doOnSuccess(result -> { // 仅处理成功结果的日志和监控
                                Instant endTime = Instant.now();
                                Duration totalDuration = Duration.between(nodeProcessingStartTime, endTime);
                                Duration logicDuration = Duration.between(logicStartTime, endTime);
                                // 验证结果类型
                                validateResultTypes(result, node, nodeName, dagName, requestId);
                                if (result.isSuccess()) {
                                    safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagName, nodeName, totalDuration, logicDuration, result, node));
                                } else {
                                    // 如果 execute 返回的 Mono 成功了，但结果内部状态不是 SUCCESS
                                    Throwable err = result.getError().orElseGet(() -> new NodeExecutionException("Node execute() returned a non-SUCCESS result without an error", nodeName, dagName));
                                    log.warn("[RequestId: {}] DAG '{}': 节点 '{}' execute() 成功返回但结果状态为 {}，按失败处理。", requestId, dagName, nodeName, result.getStatus());
                                    // 注意：这里无法直接触发 onErrorResume，需要确保这种情况被视为错误
                                    // 最好是在 NodeResult 工厂或 execute 实现中避免这种情况
                                    // 这里仅记录日志，失败处理依赖 onErrorResume
                                    safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, err, node));
                                }
                            });
                    // 注意：doOnError 不在这里放，放到 retryWhen 和 onErrorResume 之后处理最终错误
                })
                .retryWhen(retrySpec != null ? retrySpec : Retry.max(0)) // 应用重试策略
                .doOnError(error -> { // 重试耗尽或首次执行失败（非超时）会到这里
                    if (!(error instanceof TimeoutException)) { // 超时有单独处理
                        log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试失败 (非超时): {}", requestId, dagName, nodeName, error.getMessage());
                    }
                    // 实际的失败通知和结果创建在 onErrorResume 中
                })
                .onErrorResume(error -> { // 统一错误处理 (包括超时和执行错误)
                    Instant endTime = Instant.now();
                    Duration totalDuration = Duration.between(nodeProcessingStartTime, endTime);
                    // 尝试使用 logicStartTime，如果失败发生在 execute 之前，则 logicDuration 可能不准确或为0
                    Duration logicDuration = Duration.between(logicStartTime, endTime);

                    if (error instanceof TimeoutException) {
                        log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行最终超时 ({}).", requestId, dagName, nodeName, timeout);
                        safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagName, nodeName, timeout, node));
                    } else {
                        log.error("[RequestId: {}] DAG '{}': 节点 '{}' 执行最终失败: {}",
                                requestId, dagName, nodeName, error.getMessage(), error); // 使用 error 级别
                    }

                    // 通知监听器失败
                    safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, error, node));

                    // 返回包含错误的 Failure NodeResult (使用原始通配符 node 创建)
                    return Mono.just(createFailureResult(context, error, node));
                })
                // 将结果类型转换回 Mono<NodeResult<C, ?, ?>>
                .map(result -> (NodeResult<C, ?, ?>) result);
    }


    /**
     * 验证节点返回的 NodeResult 的类型与其自身声明是否一致。
     */
    private <C> void validateResultTypes(
            NodeResult<C, ?, ?> result, // 接收通配符结果
            DagNode<C, ?, ?> node,    // 接收通配符节点
            String nodeName,
            String dagName,
            String requestId) {

        Class<?> declaredPayloadType = node.getPayloadType(); // 从节点获取声明类型
        Class<?> declaredEventType = node.getEventType();
        Class<?> actualPayloadType = result.getPayloadType(); // 从结果获取实际类型
        Class<?> actualEventType = result.getEventType();

        // 检查 Payload 类型
        if (actualPayloadType == null || !declaredPayloadType.equals(actualPayloadType)) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 返回的 NodeResult Payload 类型 ({}) 与其声明 ({}) 不符!",
                    requestId, dagName, nodeName,
                    actualPayloadType != null ? actualPayloadType.getSimpleName() : "null",
                    declaredPayloadType.getSimpleName());
        }
        // 检查 Event 类型
        if (actualEventType == null || !declaredEventType.equals(actualEventType)) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 返回的 NodeResult Event 类型 ({}) 与其声明 ({}) 不符!",
                    requestId, dagName, nodeName,
                    actualEventType != null ? actualEventType.getSimpleName() : "null",
                    declaredEventType.getSimpleName());
        }

        // 日志记录结果状态
        if (result.isSuccess()) {
            // 修正：检查返回的 Flux 是否为 Flux.empty() 的单例实例
            // 这表明 NodeResult 工厂方法没有显式添加事件流，或者节点执行明确返回了 Flux.empty()
            boolean isDefaultEmptyFlux = result.getEvents() == (Flux<?>) Flux.empty(); // 使用引用比较 ==
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Declared P: {}, E: {}) 执行成功. Result P: {}, E: {}, Payload: {}, IsDefaultEmptyFlux: {}", // 修改日志字段名
                    requestId, dagName, nodeName,
                    declaredPayloadType.getSimpleName(), declaredEventType.getSimpleName(),
                    actualPayloadType != null ? actualPayloadType.getSimpleName() : "null",
                    actualEventType != null ? actualEventType.getSimpleName() : "null",
                    result.getPayload().isPresent() ? "Present" : "Empty",
                    isDefaultEmptyFlux ? "Yes" : "No"); // 使用修正后的布尔值
        }
        // 失败和跳过状态的日志已在其他地方处理
    }


    // --- 辅助方法 ---
    private <C> Duration determineNodeTimeout(DagNode<C, ?, ?> node) {
        Duration nodeTimeout = node.getExecutionTimeout();
        return (nodeTimeout != null && !nodeTimeout.isNegative() && !nodeTimeout.isZero())
                ? nodeTimeout
                : defaultNodeTimeout;
    }

    // 辅助方法创建 Failure NodeResult (使用通配符节点)
    private <C> NodeResult<C, ?, ?> createFailureResult(C context, Throwable error, DagNode<C, ?, ?> node) {
        return callNodeResultFactory(context, node, (ctx, n) -> NodeResult.failure(ctx, error, n));
    }

    // 辅助方法创建 Skipped NodeResult (使用通配符节点)
    private <C> NodeResult<C, ?, ?> createSkippedResult(C context, DagNode<C, ?, ?> node) {
        return callNodeResultFactory(context, node, NodeResult::skipped);
    }

    // 使用泛型辅助方法来调用 NodeResult 的静态工厂，处理类型转换
    @FunctionalInterface
    private interface NodeResultFactory<C, N extends DagNode<C, P, T>, P, T> {
        NodeResult<C, P, T> create(C context, N node);
    }

    @SuppressWarnings("unchecked")
    private <C, P, T> NodeResult<C, ?, ?> callNodeResultFactory(C context, DagNode<C, ?, ?> node, NodeResultFactory<C, DagNode<C, P, T>, P, T> factory) {
        try {
            // 强制转换节点类型以匹配工厂方法签名
            DagNode<C, P, T> typedNode = (DagNode<C, P, T>) node;
            return factory.create(context, typedNode);
        } catch (ClassCastException e) {
            // 如果类型转换失败（理论上不应发生，因为 node 是从 definition 获取的）
            log.error("Internal error: Failed to cast DagNode to expected type for NodeResult factory. Node: {}, Target Type Hint (may be incorrect): P={}, T={}",
                    node.getName(), "P?", "T?", e);
            // 返回一个通用的错误结果
            return createFailureResult(context, new IllegalStateException("Internal type casting error for NodeResult factory", e), node);
        }
    }


    // 安全地通知监听器
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

    // 自定义异常类
    private static class NodeExecutionException extends RuntimeException {
        public NodeExecutionException(String message, String nodeName, String dagName) {
            super(String.format("[%s] Node '%s': %s", dagName, nodeName, message));
        }
        // public NodeExecutionException(String message, Throwable cause, String nodeName, String dagName) {
        //     super(String.format("[%s] Node '%s': %s", dagName, nodeName, message), cause);
        // }
    }
    private static class NodeConfigurationException extends RuntimeException {
        public NodeConfigurationException(String message, Throwable cause, String nodeName, String dagName) {
            super(String.format("[%s] Node '%s' Configuration Error: %s", dagName, nodeName, message), cause);
        }
    }
}
