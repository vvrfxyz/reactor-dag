package xyz.vvrf.reactor.dag.impl;

import xyz.vvrf.reactor.dag.core.DependencyAccessor;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.core.*;
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;
import java.time.Instant;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * 标准节点执行器 - 负责执行单个节点并处理依赖关系和缓存。
 * 支持流式节点：不等待依赖节点的事件流完成，直接传递包含流引用的 NodeResult。
 * 使用 DagDefinition.getEffectiveDependencies 获取依赖。
 *
 * @author ruifeng.wen
 */
@Slf4j
public class StandardNodeExecutor {

    private final Duration defaultNodeTimeout;
    private final Scheduler nodeExecutionScheduler;
    private final List<DagMonitorListener> monitorListeners; // 添加监听器列表字段

    /**
     * 创建标准节点执行器，使用默认的 Schedulers.boundedElastic()。
     *
     * @param defaultNodeTimeout      默认节点执行超时时间 (不能为空)
     */
    public StandardNodeExecutor(Duration defaultNodeTimeout) {
        this(defaultNodeTimeout, Schedulers.boundedElastic());
    }

    /**
     * 使用指定的调度器创建 StandardNodeExecutor，不带监听器。
     * @deprecated 请使用带有监听器的构造函数以获得监控能力。
     */
    @Deprecated
    public StandardNodeExecutor(Duration defaultNodeTimeout, Scheduler nodeExecutionScheduler) {
        this(defaultNodeTimeout, nodeExecutionScheduler, Collections.emptyList());
    }

    /**
     * 使用指定的超时、调度器和监控监听器创建 StandardNodeExecutor。
     *
     * @param defaultNodeTimeout      节点的默认执行超时时间 (必需)。
     * @param nodeExecutionScheduler  执行节点逻辑的调度器 (必需)。
     * @param monitorListeners        用于监控事件的监听器列表 (可以为空，但不能为 null)。
     */
    public StandardNodeExecutor(Duration defaultNodeTimeout,
                                Scheduler nodeExecutionScheduler,
                                List<DagMonitorListener> monitorListeners) {
        this.defaultNodeTimeout = Objects.requireNonNull(defaultNodeTimeout, "默认节点超时时间不能为空");
        this.nodeExecutionScheduler = Objects.requireNonNull(nodeExecutionScheduler, "节点执行调度器不能为空");
        this.monitorListeners = Objects.requireNonNull(monitorListeners, "Monitor listeners list cannot be null");

        if (defaultNodeTimeout.isNegative() || defaultNodeTimeout.isZero()) {
            log.warn("配置的 defaultNodeTimeout <= 0，节点执行可能不会超时: {}", defaultNodeTimeout);
        }

        log.info("StandardNodeExecutor 初始化完成，节点默认超时: {}, 调度器: {}, 监听器数量: {}",
                defaultNodeTimeout, nodeExecutionScheduler, monitorListeners.size());
    }

    /**
     * 获取或创建节点执行的 Mono<NodeResult>，支持请求级缓存。
     *
     * @param <C>           上下文类型
     * @param <P>           期望的节点 Payload 类型
     * @param nodeName      节点名称
     * @param payloadType   期望的节点 Payload 类型 Class 对象
     * @param context       当前上下文
     * @param cache         请求级缓存 (Key: String, Value: Mono<? extends NodeResult<C, ?, ?>>)
     * @param dagDefinition DAG 定义
     * @param requestId     请求 ID，用于日志
     * @return 返回一个 Mono，该 Mono 在订阅时会执行节点或从缓存返回结果。
     * 结果 NodeResult 的 Payload 类型为 P，Event Data 类型为通配符 ?。
     */
    public <C, P> Mono<NodeResult<C, P, ?>> getNodeExecutionMono(
            final String nodeName,
            final Class<P> payloadType,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String cacheKey = nodeName + "#" + payloadType.getName();
        final String dagName = dagDefinition.getDagName();

        // 使用 computeIfAbsent 简化缓存逻辑，确保只创建一次 Mono
        // 注意：Caffeine 的 computeIfAbsent 不是原子的对于值的计算，但对于 Mono.cache() 来说通常没问题，
        return Mono.defer(() -> {
            Mono<? extends NodeResult<C, ?, ?>> cachedMono = cache.getIfPresent(cacheKey);

            if (cachedMono != null) {
                log.trace("[RequestId: {}] DAG '{}': 缓存命中节点 '{}' (Payload 类型 {})",
                        requestId, dagName, nodeName, payloadType.getSimpleName());
                return (Mono<NodeResult<C, P, ?>>) cachedMono;
            } else {
                log.debug("[RequestId: {}] DAG '{}': 缓存未命中，创建节点 '{}' (Payload 类型 {}) 执行 Mono",
                        requestId, dagName, nodeName, payloadType.getSimpleName());

                Mono<NodeResult<C, P, ?>> newMono = createNodeExecutionMono(
                        nodeName, payloadType, context, cache, dagDefinition, requestId);

                // 使用 .cache() 操作符来确保 Mono 只执行一次并将结果缓存起来供后续订阅者使用
                Mono<? extends NodeResult<C, ?, ?>> monoToCache = newMono.cache();
                cache.put(cacheKey, monoToCache);

                // 返回缓存的 Mono，并进行类型转换
                return (Mono<NodeResult<C, P, ?>>) monoToCache;
            }
        });
    }

    /**
     * 创建节点执行的 Mono 。
     * 这个 Mono 在订阅时会查找节点、解析依赖、执行节点逻辑。
     *
     * @param <P> 期望的 Payload 类型
     * @return Mono<NodeResult < C, P, ?>> T 类型由节点决定，用 ? 表示
     */
    private <C, P> Mono<NodeResult<C, P, ?>> createNodeExecutionMono(
            final String nodeName,
            final Class<P> payloadType,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();

        return Mono.defer(() -> {
            DagNode<C, P, ?> node;
            try {
                node = findNode(nodeName, payloadType, dagDefinition, requestId);
            } catch (Exception e) {
                // 记录日志并返回错误。考虑是否需要 DAG 级别的失败事件。
                log.error("[RequestId: {}] DAG '{}': 执行前查找节点 '{}' 失败。", requestId, dagName, nodeName, e);
                return Mono.error(e);
            }
            Duration timeout = determineNodeTimeout(node);

            Instant startTime = Instant.now(); // 尽早记录开始时间
            safeNotifyListeners(l -> l.onNodeStart(requestId, dagName, nodeName, node));

            log.debug("[RequestId: {}] DAG '{}': 准备执行节点 '{}' (期望 Payload: {}, 实现: {}), 超时: {}",
                    requestId, dagName, nodeName, payloadType.getSimpleName(), node.getClass().getSimpleName(), timeout);

            // 解析依赖，获取包含依赖结果的 Map 的 Mono
            Mono<Map<String, NodeResult<C, ?, ?>>> dependenciesMono = resolveDependencies(
                    node, context, cache, dagDefinition, requestId);

            return dependenciesMono
                    .flatMap(dependencyResults -> {
                        DependencyAccessor<C> accessor = new DefaultDependencyAccessor<>(dependencyResults);

                        boolean shouldExec;
                        try {
                            shouldExec = node.shouldExecute(context, accessor);
                        } catch (Exception e) {
                            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 的 shouldExecute 方法抛出异常，将视为不执行并产生错误结果。",
                                    requestId, dagName, nodeName, e);
                            Duration duration = Duration.between(startTime, Instant.now());
                            safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, duration, e, node));

                            return Mono.just(NodeResult.failure(context,
                                    new IllegalStateException("shouldExecute failed for node " + nodeName, e),
                                    node));
                        }

                        if (shouldExec) {
                            // 条件满足，执行节点 (包含重试逻辑)
                            log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 条件满足，将执行。", requestId, dagName, nodeName);
                            // executeNodeInternal 返回 Mono<NodeResult<C, P, T>>
                            Mono<? extends NodeResult<C, P, ?>> resultMono = executeNodeInternal(
                                    node, context, dependencyResults, timeout, requestId, dagName,startTime);
                            return resultMono;
                        } else {
                            // 条件不满足，跳过执行
                            log.info("[RequestId: {}] DAG '{}': 节点 '{}' 条件不满足，将被跳过。", requestId, dagName, nodeName);
                            safeNotifyListeners(l -> l.onNodeSkipped(requestId, dagName, nodeName, node));
                            NodeResult<C, P, ?> skippedResult = NodeResult.skipped(context, node);
                            return Mono.just(skippedResult);
                        }
                    })
                    // 添加顶层错误处理，用于依赖解析失败等情况
                    .onErrorResume(error -> {
                        log.error("[RequestId: {}] DAG '{}': 节点 '{}' 在执行前或依赖解析中失败: {}", requestId, dagName, nodeName, error.getMessage(), error);
                        Duration duration = Duration.between(startTime, Instant.now());
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, duration, error, node));
                        // 如果依赖解析早期失败，这里可能没有 context。
                        // 如果可能，创建一个失败结果。
                        return Mono.just(NodeResult.failure(context, error, node));
                    });
        });
    }

    /**
     * 查找节点实例，并验证其 Payload 类型。
     */
    private <C, P> DagNode<C, P, ?> findNode(
            String nodeName,
            Class<P> expectedPayloadType,
            DagDefinition<C> dagDefinition,
            String requestId) {
        return dagDefinition.getNode(nodeName, expectedPayloadType)
                .orElseThrow(() -> {
                    String dagName = dagDefinition.getDagName();
                    Set<Class<?>> availableTypes = dagDefinition.getSupportedOutputTypes(nodeName);
                    String errorMsg;
                    if (availableTypes.isEmpty()) {
                        errorMsg = String.format("节点 '%s' 在 DAG '%s' 中不存在。", nodeName, dagName);
                    } else {
                        errorMsg = String.format("节点 '%s' 在 DAG '%s' 中不支持 Payload 类型 '%s'。支持的 Payload 类型: %s",
                                nodeName, dagName, expectedPayloadType.getSimpleName(),
                                availableTypes.stream().map(Class::getSimpleName).collect(Collectors.toList()));
                    }
                    log.error("[RequestId: {}] {}", requestId, errorMsg);
                    return new IllegalStateException(errorMsg);
                });
    }

    /**
     * 确定节点的实际执行超时时间。
     */
    private <C, P, T> Duration determineNodeTimeout(DagNode<C, P, T> node) {
        Duration nodeTimeout = node.getExecutionTimeout();
        if (nodeTimeout != null && !nodeTimeout.isZero() && !nodeTimeout.isNegative()) {
            return nodeTimeout;
        }
        return defaultNodeTimeout;
    }

    /**
     * 解析节点的所有依赖 (使用 DagDefinition.getEffectiveDependencies)。
     * 返回一个 Mono，该 Mono 完成时会发出一个 Map，包含所有依赖节点的名称及其对应的 NodeResult。
     */
    private <C, P, T> Mono<Map<String, NodeResult<C, ?, ?>>> resolveDependencies(
            final DagNode<C, P, T> node, // 传入节点实例是为了获取名称，也可以只传名称
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String nodeName = node.getName();
        final String dagName = dagDefinition.getDagName();

        // *** 使用 getEffectiveDependencies 获取依赖列表 ***
        List<DependencyDescriptor> dependencies = dagDefinition.getEffectiveDependencies(nodeName);

        if (dependencies.isEmpty()) {
            return Mono.just(Collections.emptyMap());
        }

        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 正在解析 {} 个有效依赖: {}",
                requestId, dagName, nodeName, dependencies.size(),
                dependencies.stream().map(DependencyDescriptor::toString).collect(Collectors.toList()));

        // 为每个依赖创建一个获取其 NodeResult<C, ?, ?> 的 Mono<Map.Entry>
        List<Mono<Map.Entry<String, NodeResult<C, ?, ?>>>> dependencyMonos = dependencies.stream()
                .map(dep -> resolveSingleDependency(dep, context, cache, dagDefinition, nodeName, requestId))
                .collect(Collectors.toList());

        // 使用 Flux.flatMap 并发执行所有依赖解析 Mono
        // collectMap 会等待所有内部 Mono<Map.Entry> 完成后，将结果收集到 Map 中
        return Flux.fromIterable(dependencyMonos)
                .flatMap(mono -> mono) // 触发每个 Mono<Map.Entry> 的执行
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .doOnSuccess(results -> log.debug(
                        "[RequestId: {}] DAG '{}': 节点 '{}' 的所有 {} 个依赖的 NodeResult 已就绪。",
                        requestId, dagName, nodeName, results.size()))
                .doOnError(e -> log.error(
                        "[RequestId: {}] DAG '{}': 节点 '{}' 获取依赖 NodeResult 失败: {}",
                        requestId, dagName, nodeName, e.getMessage(), e));
    }

    /**
     * 解析单个依赖：递归调用 getNodeExecutionMono 获取依赖节点的 Mono<NodeResult>，
     * 并在 Mono 完成后直接将其结果包装成 Map.Entry。不等待事件流。
     */
    @SuppressWarnings("unchecked") // for requiredPayloadType cast
    private <C, R> Mono<Map.Entry<String, NodeResult<C, ?, ?>>> resolveSingleDependency(
            DependencyDescriptor dep,
            C context,
            Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
            DagDefinition<C> dagDefinition,
            String dependentNodeName,
            String requestId) {

        String depName = dep.getName();
        Class<R> requiredPayloadType = (Class<R>) dep.getRequiredType();
        String dagName = dagDefinition.getDagName();

        log.trace("[RequestId: {}] DAG '{}': 节点 '{}' 开始解析依赖 '{}' (期望 Payload: {})",
                requestId, dagName, dependentNodeName, depName, requiredPayloadType.getSimpleName());

        // 获取依赖节点的 Mono<NodeResult<C, R, ?>>
        // 这个 Mono 完成时，表示 NodeResult 对象已创建（可能包含活动流）
        Mono<NodeResult<C, R, ?>> dependencyResultMono = getNodeExecutionMono(
                depName, requiredPayloadType, context, cache, dagDefinition, requestId);

        // 当依赖节点的 Mono<NodeResult> 完成时，处理结果
        return dependencyResultMono
                .map(result -> {
                    if (result.isFailure()) {
                        log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 的依赖 '{}' 执行失败，将使用此失败结果。错误: {}",
                                requestId, dagName, dependentNodeName, depName,
                                result.getError().map(Throwable::getMessage).orElse("未知错误"));
                    } else {
                        // 依赖节点执行成功，NodeResult 已就绪（流可能正在进行）
                        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 的依赖 '{}' (Payload: {}) NodeResult 已就绪 (流可能正在进行)",
                                requestId, dagName, dependentNodeName, depName, result.getPayloadType().getSimpleName());
                    }
                    // 直接创建 Map Entry，不等待事件流
                    // NodeResult<C, R, ?> 可以安全地转为 NodeResult<C, ?, ?> 用于 Map Value
                    return createMapEntry(depName, result);
                })
                .onErrorResume(e -> handleDependencyResolutionError(e, depName, dependentNodeName, dagDefinition, requestId));
    }

    /**
     * 创建依赖名称到其 NodeResult 的映射条目。
     * 输入 NodeResult<C, R, ?>，输出 Map.Entry<String, NodeResult<C, ?, ?>>
     */
    private <C, R> Map.Entry<String, NodeResult<C, ?, ?>> createMapEntry(String depName, NodeResult<C, R, ?> nodeResult) {
        return new AbstractMap.SimpleEntry<>(depName, nodeResult);
    }

    /**
     * 处理在获取依赖节点的 NodeResult 过程中发生的错误。
     */
    private <C> Mono<Map.Entry<String, NodeResult<C, ?, ?>>> handleDependencyResolutionError(
            Throwable e,
            String depName,
            String dependentNodeName,
            DagDefinition<C> dagDefinition,
            String requestId) {

        String dagName = dagDefinition.getDagName();
        log.error("[RequestId: {}] DAG '{}': 获取节点 '{}' 的依赖 '{}' 的 NodeResult 失败: {}",
                requestId, dagName, dependentNodeName, depName, e.getMessage(), e);
        return Mono.error(e);
    }

    /**
     * 内部方法：实际执行节点逻辑。
     * 返回 Mono<NodeResult<C, P, T>>，其中 T 是由节点决定的具体事件类型。
     */
    private <C, P, T> Mono<NodeResult<C, P, T>> executeNodeInternal(
            DagNode<C, P, T> node,
            C context,
            Map<String, NodeResult<C, ?, ?>> dependencyResultsMap,
            Duration timeout,
            String requestId,
            String dagName,
            Instant startTime) {

        String nodeName = node.getName();
        Class<P> expectedPayloadType = node.getPayloadType();
        Retry retrySpec = node.getRetrySpec();

        return Mono.defer(() -> {
                    log.info("[RequestId: {}] DAG '{}': 开始执行节点 '{}' (Payload: {}, Impl: {}) 逻辑...",
                            requestId, dagName, nodeName, expectedPayloadType.getSimpleName(), node.getClass().getSimpleName());

                    // *** 1. 创建 DependencyAccessor 实例 ***
                    DependencyAccessor<C> accessor = new DefaultDependencyAccessor<>(dependencyResultsMap);

                    // *** 2. 调用修改后的 node.execute，传入 accessor ***
                    return node.execute(context, accessor);
                })
                .subscribeOn(nodeExecutionScheduler)
                .timeout(timeout)
                .doOnSuccess(result -> {
                    // 节点成功执行 (在超时时间内，可能在重试后)
                    Duration duration = Duration.between(startTime, Instant.now());
                    validateAndLogResult(result, expectedPayloadType, nodeName, dagName, requestId);
                    // 在验证/记录日志之后通知监听器成功
                    if (result.isSuccess()) { // 再次检查结果状态
                        safeNotifyListeners(l -> l.onNodeSuccess(requestId, dagName, nodeName, duration, result));
                    } else if (result.isFailure()) {
                        // 理想情况下应由 onError 捕获，但如果 NodeResult 本身指示失败，则处理
                        Throwable error = result.getError().orElse(new RuntimeException("NodeResult indicated failure with no error object"));
                        safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, duration, error, node));
                    } else { // 跳过情况已在前面处理
                        log.warn("[RequestId: {}] DAG '{}': Node '{}' finished in success path but result is not success/failure? Status: {}",
                                requestId, dagName, nodeName, result.getStatus());
                    }
                })
                .doOnError(error -> {
                    if (!(error instanceof TimeoutException)) {
                        log.warn(
                                "[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试失败 (非超时): {}",
                                requestId, dagName, nodeName, error.getMessage(), error);
                        safeNotifyListeners(l -> l.onNodeTimeout(requestId, dagName, nodeName, timeout, node));
                    } else {
                        log.warn(
                                "[RequestId: {}] DAG '{}': 节点 '{}' 执行尝试超时 ({}).",
                                requestId, dagName, nodeName, timeout);
                    }
                })
                .retryWhen(retrySpec != null ? retrySpec : Retry.max(0))
                .onErrorResume(error -> {
                    // 节点执行最终失败 (在重试后，或立即失败)
                    Duration duration = Duration.between(startTime, Instant.now());
                    log.error("[RequestId: {}] DAG '{}': 节点 '{}' 执行失败 (重试耗尽或不可重试): {}",
                            requestId, dagName, nodeName, error.getMessage(), error);
                    // 通知监听器最终的失败
                    safeNotifyListeners(l -> l.onNodeFailure(requestId, dagName, nodeName, duration, error, node));
                    // 构建并返回一个失败的 NodeResult
                    return Mono.just(NodeResult.failure(context, error, node));
                });
    }

    /**
     * 验证节点返回的 NodeResult 并记录日志。
     */
    private <C, P, T> void validateAndLogResult(
            NodeResult<C, P, T> result,
            Class<P> expectedPayloadType,
            String nodeName,
            String dagName,
            String requestId) {

        // 验证 Payload 类型是否匹配
        // 注意：对于流式节点，Payload 可能为 null 或 Optional.empty()，需要 NodeResult 内部处理好 getPayloadType()
        if (result.getPayloadType() == null) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 返回的 NodeResult 的 Payload 类型为 null!",
                    requestId, dagName, nodeName);
        } else if (!expectedPayloadType.equals(result.getPayloadType())) {
            // 允许子类兼容，改为 isAssignableFrom 检查
            if (!expectedPayloadType.isAssignableFrom(result.getPayloadType())) {
                log.error("[RequestId: {}] DAG '{}': 节点 '{}' 返回的 NodeResult 的 Payload 类型 ({}) 与其声明的 Payload 类型 ({}) 不兼容!",
                        requestId, dagName, nodeName,
                        result.getPayloadType().getSimpleName(),
                        expectedPayloadType.getSimpleName());
            } else {
                log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 返回的 Payload 类型 ({}) 是声明类型 ({}) 的子类或实现。",
                        requestId, dagName, nodeName,
                        result.getPayloadType().getSimpleName(),
                        expectedPayloadType.getSimpleName());
            }
        }

        if (result.isSuccess()) {
            boolean hasEvents = result.getEvents() != null && result.getEvents() != Flux.<Event<T>>empty();
            log.info("[RequestId: {}] DAG '{}': 节点 '{}' (期望 Payload: {}) 执行成功. Actual Payload Type: {}, Payload: {}, HasEvents: {}",
                    requestId, dagName, nodeName, expectedPayloadType.getSimpleName(),
                    result.getPayloadType() != null ? result.getPayloadType().getSimpleName() : "null",
                    result.getPayload().isPresent() ? "Present" : "Empty",
                    hasEvents ? "Yes" : "No");
        } else {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' (期望 Payload: {}) 执行完成，但返回了错误: {}",
                    requestId, dagName, nodeName, expectedPayloadType.getSimpleName(),
                    result.getError().map(Throwable::getMessage).orElse("未知错误"));
        }
    }

    /**
     * 安全地通知所有注册的监听器，捕获单个监听器的异常。
     *
     * @param notification 要对每个监听器执行的操作。
     */
    private void safeNotifyListeners(java.util.function.Consumer<DagMonitorListener> notification) {
        if (monitorListeners.isEmpty()) {
            return;
        }
        for (DagMonitorListener listener : monitorListeners) {
            try {
                notification.accept(listener);
            } catch (Exception e) {
                log.error("DAG Monitor Listener {} 抛出异常: {}", listener.getClass().getName(), e.getMessage(), e);
                // 继续通知其他监听器
            }
        }
    }
}
