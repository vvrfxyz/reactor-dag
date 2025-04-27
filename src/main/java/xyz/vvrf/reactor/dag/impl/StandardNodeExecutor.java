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
 * 使用 DagDefinition 获取执行前驱和输入映射。
 * **修改**:
 * - 接收所有已完成节点的结果 (`allCompletedResults`) 来创建 `InputDependencyAccessor`。
 * - `getNodeExecutionMono` 不再关心 P 类型，返回 `Mono<NodeResult<C, ?, ?>>`。
 * - 依赖解析 (`resolveDependencies`) 确保执行前驱完成，但不直接收集结果给 Accessor。
 *
 * @author ruifeng.wen (Refactored by Devin)
 */
@Slf4j
public class StandardNodeExecutor {

    private final Duration defaultNodeTimeout;
    private final Scheduler nodeExecutionScheduler;
    private final List<DagMonitorListener> monitorListeners; // 监听器列表

    public StandardNodeExecutor(Duration defaultNodeTimeout,
                                Scheduler nodeExecutionScheduler,
                                List<DagMonitorListener> monitorListeners) { // 接收监听器列表
        this.defaultNodeTimeout = Objects.requireNonNull(defaultNodeTimeout, "默认节点超时时间不能为空");
        this.nodeExecutionScheduler = Objects.requireNonNull(nodeExecutionScheduler, "节点执行调度器不能为空");
        // 确保列表不为 null，并创建不可变副本
        this.monitorListeners = Collections.unmodifiableList(new ArrayList<>(
                Objects.requireNonNull(monitorListeners, "Monitor listeners list cannot be null")));

        if (defaultNodeTimeout.isNegative() || defaultNodeTimeout.isZero()) {
            log.warn("配置的 defaultNodeTimeout <= 0，节点执行可能不会超时: {}", defaultNodeTimeout);
        }

        log.info("StandardNodeExecutor 初始化完成，节点默认超时: {}, 调度器: {}, 监听器数量: {}",
                defaultNodeTimeout, nodeExecutionScheduler, this.monitorListeners.size());
    }

    // 保留其他构造函数，确保它们调用主构造函数并传递空列表
    public StandardNodeExecutor(Duration defaultNodeTimeout) {
        this(defaultNodeTimeout, Schedulers.boundedElastic(), Collections.emptyList());
    }

    @Deprecated // 标记旧构造函数为弃用
    public StandardNodeExecutor(Duration defaultNodeTimeout, Scheduler nodeExecutionScheduler) {
        this(defaultNodeTimeout, nodeExecutionScheduler, Collections.emptyList());
    }


    /**
     * 获取或创建节点执行的 Mono<NodeResult>，支持请求级缓存 Mono 执行逻辑。
     * 此方法返回的 Mono 在订阅时会：
     * 1. 检查缓存中是否已有此节点的执行 Mono。
     * 2. 如果没有，则创建一个新的 Mono，该 Mono 会：
     *    a. 查找节点实例。
     *    b. 解析并等待所有 *执行* 前驱完成（利用缓存）。
     *    c. 基于 *所有已完成* 节点的结果创建 InputDependencyAccessor。
     *    d. 调用节点的 shouldExecute 和 execute 方法。
     *    e. 处理超时、重试和错误。
     * 3. 将新创建的 Mono 放入缓存（使用 .cache()）。
     * 4. 返回缓存的或新创建的 Mono。
     *
     * @param <C>                 上下文类型
     * @param nodeName            节点名称
     * @param context             当前上下文
     * @param monoCache           请求级缓存 (Key: String, Value: Mono<? extends NodeResult<C, ?, ?>>)，用于缓存执行 Mono
     * @param allCompletedResults 当前执行中所有已完成节点的结果映射
     * @param dagDefinition       DAG 定义
     * @param requestId           请求 ID，用于日志
     * @return 返回一个 Mono<NodeResult<C, ?, ?>>，其具体的 P 和 T 类型由节点自身决定。
     */
    public <C> Mono<NodeResult<C, ?, ?>> getNodeExecutionMono(
            final String nodeName,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> monoCache, // Renamed for clarity
            final Map<String, NodeResult<C, ?, ?>> allCompletedResults, // Receive all results
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String cacheKey = nodeName; // 使用 nodeName 作为缓存 Key
        final String dagName = dagDefinition.getDagName();

        // 使用 computeIfAbsent 原子地获取或创建缓存项
        // 注意：Caffeine 的 computeIfAbsent 不是原子的，get/put 更适合
        // 这里使用 get + compute 实现类似效果，但有轻微竞态可能（两个线程同时miss，都创建mono，只有一个放入）
        // 对于 Mono.cache() 来说，这通常没问题，因为最终只有一个会真正执行并缓存结果。

        Mono<? extends NodeResult<C, ?, ?>> cachedMono = monoCache.getIfPresent(cacheKey);
        if (cachedMono != null) {
            log.trace("[RequestId: {}] DAG '{}': 缓存命中节点 '{}' 的执行 Mono",
                    requestId, dagName, nodeName);
            // 直接返回缓存的 Mono，类型是通配符，调用者需要处理
            return Mono.from(cachedMono); // Use Mono.from to ensure correct publisher type
        } else {
            log.debug("[RequestId: {}] DAG '{}': 缓存未命中，创建节点 '{}' 执行 Mono",
                    requestId, dagName, nodeName);

            // 创建新的执行 Mono，传递 allCompletedResults
            Mono<NodeResult<C, ?, ?>> newMono = createNodeExecutionMonoInternal(
                    nodeName, context, monoCache, allCompletedResults, dagDefinition, requestId);

            // 使用 .cache() 缓存结果，并放入缓存
            // .cache() 确保底层逻辑只执行一次
            Mono<NodeResult<C, ?, ?>> monoToCache = newMono.cache();
            monoCache.put(cacheKey, monoToCache); // 放入缓存

            // 返回这个会被缓存的 Mono
            return monoToCache;
        }
    }

    // 移除 castResultMono，因为 getNodeExecutionMono 现在返回通配符类型

    /**
     * 创建节点执行的 Mono (内部实现)。
     * 这个 Mono 在订阅时会查找节点、解析依赖、执行节点逻辑。
     */
    private <C> Mono<NodeResult<C, ?, ?>> createNodeExecutionMonoInternal(
            final String nodeName,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> monoCache,
            final Map<String, NodeResult<C, ?, ?>> allCompletedResults, // 接收所有结果
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();

        // 使用 Mono.defer 确保每次订阅时都执行查找和依赖解析逻辑
        return Mono.defer(() -> {
            // 1. 获取节点实例 (不关心具体类型 P, T)
            final DagNode<C, ?, ?> node; // Make final for use in lambdas
            try {
                node = dagDefinition.getNodeAnyType(nodeName)
                        .orElseThrow(() -> new IllegalStateException(
                                String.format("Node '%s' not found in DAG '%s' during execution preparation.", nodeName, dagName)));
            } catch (Exception e) {
                log.error("[RequestId: {}] DAG '{}': 执行前查找节点 '{}' 失败。", requestId, dagName, nodeName, e);
                // 返回一个立即失败的 Mono，并包含一个表示配置错误的 NodeResult
                return Mono.just(createFailureResultForConfigError(context, e, nodeName, dagDefinition));
            }

            // 2. 确定超时
            Duration timeout = determineNodeTimeout(node);
            Instant nodeStartTime = Instant.now(); // 记录节点处理开始时间 (包括依赖解析)
            safeNotifyListeners(l -> l.onNodeStart(requestId, dagName, nodeName, node)); // 通知开始

            log.debug("[RequestId: {}] DAG '{}': 准备执行节点 '{}' (Impl: {}), 超时: {}",
                    requestId, dagName, nodeName, node.getClass().getSimpleName(), timeout);

            // 3. 解析并等待 *执行* 前驱完成
            Mono<Void> predecessorsCompletionMono = resolveExecutionPredecessors(
                    nodeName, context, monoCache, allCompletedResults, dagDefinition, requestId);

            // 4. 在所有执行前驱完成后，执行当前节点逻辑
            return predecessorsCompletionMono
                    .then(Mono.defer(() -> { // 使用 defer 确保在 then 之后执行
                        // 5. 创建 InputDependencyAccessor
                        // **关键修改**: 使用 allCompletedResults 创建 Accessor
                        Map<InputRequirement<?>, String> nodeInputMappings = dagDefinition.getInputMappingForNode(nodeName);
                        InputDependencyAccessor<C> accessor = new DefaultInputDependencyAccessor<>(
                                allCompletedResults, // 传入所有已完成的结果
                                nodeInputMappings,
                                nodeName,
                                dagName);

                        // 6. 检查 shouldExecute
                        boolean shouldExec;
                        try {
                            shouldExec = node.shouldExecute(context, accessor);
                        } catch (Exception e) {
                            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 的 shouldExecute 方法抛出异常，将视为不执行并产生错误结果。",
                                    requestId, dagName, nodeName, e);
                            Duration totalDuration = Duration.between(nodeStartTime, Instant.now());
                            // 通知失败
                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, Duration.ZERO, e, node));
                            // 返回表示错误的 NodeResult
                            return Mono.just(createFailureResult(context, e, node));
                        }

                        // 7. 执行或跳过
                        if (shouldExec) {
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 条件满足，将执行。", requestId, dagName, nodeName);
                            // 调用 executeNodeLogic，传递新的 Accessor
                            // 注意：executeNodeLogic 现在接收通配符 node
                            Mono<NodeResult<C, ?, ?>> resultMono = executeNodeLogic(
                                    node, context, accessor, timeout, requestId, dagName, nodeStartTime);
                            return resultMono;
                        } else {
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 条件不满足，将被跳过。", requestId, dagName, nodeName);
                            Duration totalDuration = Duration.between(nodeStartTime, Instant.now());
                            // 通知跳过
                            safeNotifyListeners(l -> l.onNodeSkipped(requestId, dagName, nodeName, node));
                            // 返回 Skipped NodeResult
                            NodeResult<C, ?, ?> skippedResult = createSkippedResult(context, node);
                            return Mono.just(skippedResult);
                        }
                    }))
                    // 捕获前驱解析或 shouldExecute/execute 过程中的错误
                    .onErrorResume(error -> {
                        // 检查是否是前面 shouldExecute 抛出的异常（已经被包装在 Mono.just(failureResult) 中）
                        // 如果是其他错误（如前驱解析失败），则记录并创建失败结果
                        if (!(error instanceof NodeExecutionException)) { // 假设 shouldExecute 包装了异常
                            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 在执行前或前驱解析中失败: {}", requestId, dagName, nodeName, error.getMessage(), error);
                            Duration totalDuration = Duration.between(nodeStartTime, Instant.now());
                            // 通知失败
                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, Duration.ZERO, error, node));
                            // 返回 Failure NodeResult
                            return Mono.just(createFailureResult(context, error, node));
                        } else {
                            // 如果是已经处理过的失败，直接传播
                            return Mono.error(error);
                        }
                    });
        });
    }

    /**
     * 解析并等待节点的所有直接 *执行* 前驱完成。
     * 返回一个 Mono<Void>，该 Mono 在所有前驱的执行 Mono 完成后才完成。
     * 利用 getNodeExecutionMono 来获取前驱节点的 Mono，从而复用缓存逻辑。
     * **修改**: 不再收集前驱结果，只确保它们完成。
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
            return Mono.empty(); // 没有前驱，直接完成
        }

        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 正在等待 {} 个执行前驱完成: {}",
                requestId, dagName, nodeName, predecessorNames.size(), predecessorNames);

        // 获取所有前驱节点的执行 Mono
        List<Mono<?>> predecessorMonos = predecessorNames.stream()
                .map(predName -> getNodeExecutionMono( // 递归调用以利用缓存
                        predName,
                        context,
                        monoCache,
                        allCompletedResults, // 传递当前的累积结果
                        dagDefinition,
                        requestId
                ))
                .collect(Collectors.toList());

        // 使用 Mono.when 等待所有前驱 Mono 完成 (成功或失败)
        return Mono.when(predecessorMonos)
                .doOnSuccess(v -> log.debug(
                        "[RequestId: {}] DAG '{}': 节点 '{}' 的所有 {} 个执行前驱已完成。",
                        requestId, dagName, nodeName, predecessorNames.size()))
                .doOnError(e -> log.error(
                        "[RequestId: {}] DAG '{}': 节点 '{}' 等待前驱完成时出错 (可能是某个前驱失败): {}",
                        requestId, dagName, nodeName, e.getMessage(), e)); // 错误会在上层处理
    }


    /**
     * 内部方法：实际执行节点逻辑。
     * @param node 节点实例 (通配符类型)
     * @param accessor 依赖访问器 (基于所有已完成结果)
     * @return Mono<NodeResult<C, ?, ?>>
     */
    private <C> Mono<NodeResult<C, ?, ?>> executeNodeLogic(
            DagNode<C, ?, ?> node, // 接收通配符类型
            C context,
            InputDependencyAccessor<C> accessor,
            Duration timeout,
            String requestId,
            String dagName,
            Instant nodeStartTime) { // 接收节点处理开始时间

        String nodeName = node.getName();
        Retry retrySpec = node.getRetrySpec();

        // 使用 Mono.deferContextual 传递计时信息
        return Mono.deferContextual(contextView -> {
                    Instant logicStartTime = Instant.now(); // 记录核心逻辑开始时间
                    log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Impl: {}) 核心逻辑开始执行...",
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
                            .doOnEach(signal -> { // 通用的日志和监控点
                                // 尝试从 ContextView 获取逻辑开始时间
                                signal.getContextView().<Instant>getOrEmpty("logicStartTime")
                                        .ifPresent(lStartTime -> {
                                            Instant endTime = Instant.now();
                                            Duration totalDuration = Duration.between(nodeStartTime, endTime); // 总时长
                                            Duration logicDuration = Duration.between(lStartTime, endTime); // 逻辑时长

                                            if (signal.isOnNext()) {
                                                @SuppressWarnings("unchecked")
                                                NodeResult<C, ?, ?> result = (NodeResult<C, ?, ?>) signal.get();
                                                // 验证结果类型 (相对于节点自身声明的类型)
                                                validateResultTypes(result, node, nodeName, dagName, requestId);
                                                if (result.isSuccess()) {
                                                    safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagName, nodeName, totalDuration, logicDuration, result, node));
                                                } else {
                                                    // 如果 onNext 收到非成功结果 (例如节点内部返回 failure)
                                                    Throwable err = result.getError().orElseGet(() -> new NodeExecutionException("NodeResult in onNext indicated failure with no error object", nodeName, dagName));
                                                    log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 在 doOnEach 的 onNext 中收到非成功结果 (Status: {}), 按失败处理。", requestId, dagName, nodeName, result.getStatus());
                                                    safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, err, node));
                                                }
                                            } else if (signal.isOnError()) {
                                                Throwable error = signal.getThrowable();
                                                if (error instanceof TimeoutException) {
                                                    log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试超时 ({}).", requestId, dagName, nodeName, timeout);
                                                    safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagName, nodeName, timeout, node));
                                                    // 失败通知在下面的 onErrorResume 中处理
                                                } else {
                                                    log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试失败: {}", requestId, dagName, nodeName, error.getMessage());
                                                    // 失败通知在下面的 onErrorResume 中处理
                                                }
                                            }
                                            // onComplete 时不需要特殊处理
                                        });
                            })
                            .retryWhen(retrySpec != null ? retrySpec : Retry.max(0)) // 应用重试策略
                            .onErrorResume(error -> Mono.deferContextual(errorContextView -> { // 统一错误处理
                                Instant endTime = Instant.now();
                                // 从 ContextView 获取逻辑开始时间，如果不存在则用节点开始时间
                                Instant lStartTime = errorContextView.<Instant>getOrDefault("logicStartTime", nodeStartTime);
                                Duration totalDuration = Duration.between(nodeStartTime, endTime);
                                Duration logicDuration = Duration.between(lStartTime, endTime);

                                log.error("[RequestId: {}] DAG '{}': 节点 '{}' 执行最终失败: {}",
                                        requestId, dagName, nodeName, error.getMessage(), error); // 使用 error 级别

                                // 通知监听器失败
                                safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, totalDuration, logicDuration, error, node));

                                // 返回包含错误的 Failure NodeResult (使用原始通配符 node 创建)
                                return Mono.just(createFailureResult(context, error, node));
                            }))
                            // 将 logicStartTime 放入 ContextView
                            .contextWrite(ctx -> ctx.put("logicStartTime", logicStartTime));
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
            // 这里可以选择是否抛异常，当前仅警告
        }
        // 检查 Event 类型
        if (actualEventType == null || !declaredEventType.equals(actualEventType)) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 返回的 NodeResult Event 类型 ({}) 与其声明 ({}) 不符!",
                    requestId, dagName, nodeName,
                    actualEventType != null ? actualEventType.getSimpleName() : "null",
                    declaredEventType.getSimpleName());
            // 这里可以选择是否抛异常，当前仅警告
        }

        // 日志记录结果状态
        if (result.isSuccess()) {
            // 检查事件流是否为空引用 Flux.empty()
            boolean hasEvents = result.getEvents() != Flux.empty();
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Declared P: {}, E: {}) 执行成功. Result P: {}, E: {}, Payload: {}, HasEvents: {}",
                    requestId, dagName, nodeName,
                    declaredPayloadType.getSimpleName(), declaredEventType.getSimpleName(),
                    actualPayloadType != null ? actualPayloadType.getSimpleName() : "null",
                    actualEventType != null ? actualEventType.getSimpleName() : "null",
                    result.getPayload().isPresent() ? "Present" : "Empty",
                    hasEvents ? "Yes" : "No");
        } else if (result.isFailure()) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' (Declared P: {}, E: {}) 执行完成但返回失败状态: {}",
                    requestId, dagName, nodeName,
                    declaredPayloadType.getSimpleName(), declaredEventType.getSimpleName(),
                    result.getError().map(Throwable::getMessage).orElse("未知错误"));
        } else if (result.isSkipped()) {
            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' (Declared P: {}, E: {}) 被跳过.",
                    requestId, dagName, nodeName,
                    declaredPayloadType.getSimpleName(), declaredEventType.getSimpleName());
        }
    }


    // --- 辅助方法 ---
    private <C> Duration determineNodeTimeout(DagNode<C, ?, ?> node) {
        Duration nodeTimeout = node.getExecutionTimeout();
        // 使用默认超时如果节点未指定或指定无效
        return (nodeTimeout != null && !nodeTimeout.isNegative() && !nodeTimeout.isZero())
                ? nodeTimeout
                : defaultNodeTimeout;
    }

    // 辅助方法创建 Failure NodeResult (使用通配符节点)
    private <C> NodeResult<C, ?, ?> createFailureResult(C context, Throwable error, DagNode<C, ?, ?> node) {
        // 需要强制转换回具体类型来调用工厂方法
        return callNodeResultFactory(context, node, (ctx, n) -> NodeResult.failure(ctx, error, n));
    }

    // 辅助方法创建 Skipped NodeResult (使用通配符节点)
    private <C> NodeResult<C, ?, ?> createSkippedResult(C context, DagNode<C, ?, ?> node) {
        // 需要强制转换回具体类型来调用工厂方法
        return callNodeResultFactory(context, node, NodeResult::skipped);
    }

    // 辅助方法处理配置错误（节点查找失败等）
    private <C> NodeResult<C, ?, ?> createFailureResultForConfigError(C context, Throwable error, String nodeName, DagDefinition<C> dagDefinition) {
        // 无法获取节点实例，无法知道 P, T 类型。
        // 返回一个通用的失败结果，或者抛出更严重的异常？
        // 暂时返回一个特殊标记的失败结果，但没有正确的类型信息。
        log.error("[{}] DAG '{}': 配置错误导致无法创建节点 '{}' 的失败结果: {}", dagDefinition.getDagName(), nodeName, error.getMessage());
        // 尝试使用 Object.class 作为占位符，但这并不理想
        return new NodeResult<>(
                nodeName,
                context,
                Optional.empty(),
                Flux.empty(),
                new NodeConfigurationException("Failed to find node '" + nodeName + "': " + error.getMessage(), error, nodeName, dagDefinition.getDagName()),
                Object.class, // Placeholder type
                Object.class, // Placeholder type
                NodeResult.NodeStatus.FAILURE
        );
    }


    // 使用泛型辅助方法来调用 NodeResult 的静态工厂，处理类型转换
    @FunctionalInterface
    private interface NodeResultFactory<C, N extends DagNode<C, P, T>, P, T> {
        NodeResult<C, P, T> create(C context, N node);
    }

    @SuppressWarnings("unchecked")
    private <C, P, T> NodeResult<C, ?, ?> callNodeResultFactory(C context, DagNode<C, ?, ?> node, NodeResultFactory<C, DagNode<C, P, T>, P, T> factory) {
        // 强制转换节点类型以匹配工厂方法签名
        DagNode<C, P, T> typedNode = (DagNode<C, P, T>) node;
        return factory.create(context, typedNode);
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
                // 记录监听器本身的异常，但不影响 DAG 执行
                log.error("DAG Monitor Listener {} 在处理通知时抛出异常: {}", listener.getClass().getName(), e.getMessage(), e);
            }
        }
    }

    // 自定义异常类
    private static class NodeExecutionException extends RuntimeException {
        public NodeExecutionException(String message, String nodeName, String dagName) {
            super(String.format("[%s] Node '%s': %s", dagName, nodeName, message));
        }
        public NodeExecutionException(String message, Throwable cause, String nodeName, String dagName) {
            super(String.format("[%s] Node '%s': %s", dagName, nodeName, message), cause);
        }
    }
    private static class NodeConfigurationException extends RuntimeException {
        public NodeConfigurationException(String message, Throwable cause, String nodeName, String dagName) {
            super(String.format("[%s] Node '%s' Configuration Error: %s", dagName, nodeName, message), cause);
        }
    }

}
