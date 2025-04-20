package xyz.vvrf.reactor.dag.impl;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.core.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * 标准节点执行器 - 负责执行单个节点并处理依赖关系和缓存。
 * 支持流式节点：不等待依赖节点的事件流完成，直接传递包含流引用的 NodeResult。
 *
 * @author ruifeng.wen (modified based on discussion)
 */
@Slf4j
public class StandardNodeExecutor {

    private final Duration defaultNodeTimeout; // 节点执行的默认超时时间
    private final Scheduler nodeExecutionScheduler; // 执行节点逻辑的调度器

    /**
     * 创建标准节点执行器，使用默认的 Schedulers.boundedElastic()。
     *
     * @param defaultNodeTimeout      默认节点执行超时时间 (不能为空)
     */
    public StandardNodeExecutor(Duration defaultNodeTimeout) {
        this(defaultNodeTimeout, Schedulers.boundedElastic());
    }

    /**
     * 创建标准节点执行器，允许指定调度器。
     *
     * @param defaultNodeTimeout      默认节点执行超时时间 (不能为空)
     * @param nodeExecutionScheduler  执行节点逻辑的调度器 (不能为空)
     */
    public StandardNodeExecutor(Duration defaultNodeTimeout, Scheduler nodeExecutionScheduler) {
        this.defaultNodeTimeout = Objects.requireNonNull(defaultNodeTimeout, "默认节点超时时间不能为空");
        this.nodeExecutionScheduler = Objects.requireNonNull(nodeExecutionScheduler, "节点执行调度器不能为空");

        if (defaultNodeTimeout.isNegative() || defaultNodeTimeout.isZero()) {
            log.warn("配置的 defaultNodeTimeout <= 0，节点执行可能不会超时: {}", defaultNodeTimeout);
        }

        log.info("StandardNodeExecutor 初始化完成，节点默认超时: {}, 调度器: {}",
                defaultNodeTimeout, nodeExecutionScheduler);
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
            final Class<P> payloadType, // 期望的 Payload 类型
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String cacheKey = nodeName + "#" + payloadType.getName();
        final String dagName = dagDefinition.getDagName();

        // 使用 computeIfAbsent 简化缓存逻辑，确保只创建一次 Mono
        // 注意：Caffeine 的 computeIfAbsent 不是原子的对于值的计算，但对于 Mono.cache() 来说通常没问题，
        // 因为即使并发创建了多个 newMono，最终只有一个会被放入缓存并返回，其他的会被 GC。
        // 或者保持原有的 getIfPresent + put 模式，它对于 Mono.cache() 也是线程安全的。
        // 这里保持原有模式以减少改动。
        return Mono.defer(() -> {
            Mono<? extends NodeResult<C, ?, ?>> cachedMono = cache.getIfPresent(cacheKey);

            if (cachedMono != null) {
                log.trace("[RequestId: {}] DAG '{}': 缓存命中节点 '{}' (Payload 类型 {})",
                        requestId, dagName, nodeName, payloadType.getSimpleName());
                // 类型转换仍然需要，因为缓存存储的是通配符类型
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
            final Class<P> payloadType, // 期望的 Payload 类型
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String dagName = dagDefinition.getDagName();

        return Mono.defer(() -> {
            DagNode<C, P, ?> node = findNode(nodeName, payloadType, dagDefinition, requestId);
            Duration timeout = determineNodeTimeout(node);

            log.debug("[RequestId: {}] DAG '{}': 准备执行节点 '{}' (期望 Payload: {}, 实现: {}), 超时: {}",
                    requestId, dagName, nodeName, payloadType.getSimpleName(), node.getClass().getSimpleName(), timeout);

            // 解析依赖，获取包含依赖结果的 Map 的 Mono
            Mono<Map<String, NodeResult<C, ?, ?>>> dependenciesMono = resolveDependencies(
                    node, context, cache, dagDefinition, requestId);

            // 当所有依赖的 NodeResult 就绪后 (流可能仍在进行)，执行当前节点
            return dependenciesMono
                    .flatMap(dependencyResults -> {
                        // executeNodeInternal 返回 Mono<NodeResult<C, P, T>> (T 是具体的)
                        // 我们需要返回 Mono<NodeResult<C, P, ?>>
                        Mono<? extends NodeResult<C, P, ?>> resultMono = executeNodeInternal(
                                node, context, dependencyResults, timeout, requestId, dagName);
                        return resultMono; // 返回带有通配符类型的 Mono
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
     * 解析节点的所有依赖。
     * 返回一个 Mono，该 Mono 完成时会发出一个 Map，包含所有依赖节点的名称及其对应的 NodeResult。
     * 这个 Mono 的完成仅表示所有依赖节点的 NodeResult 对象已准备好（可能包含活动的流），
     * 而不是等待这些流完成。
     */
    private <C, P, T> Mono<Map<String, NodeResult<C, ?, ?>>> resolveDependencies(
            final DagNode<C, P, T> node,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String nodeName = node.getName();
        final String dagName = dagDefinition.getDagName();
        List<DependencyDescriptor> dependencies = node.getDependencies();

        if (dependencies == null || dependencies.isEmpty()) {
            return Mono.just(Collections.emptyMap());
        }

        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 正在解析 {} 个依赖: {}",
                requestId, dagName, nodeName, dependencies.size(),
                dependencies.stream().map(DependencyDescriptor::toString).collect(Collectors.toList()));

        // 为每个依赖创建一个获取其 NodeResult<C, ?, ?> 的 Mono<Map.Entry>
        List<Mono<Map.Entry<String, NodeResult<C, ?, ?>>>> dependencyMonos = dependencies.stream()
                .map(dep -> resolveSingleDependency(dep, context, cache, dagDefinition, nodeName, requestId))
                .collect(Collectors.toList());

        // 使用 Flux.flatMap 并发（或顺序，取决于调度器和 flatMap 行为）地执行所有依赖解析 Mono
        // collectMap 会等待所有内部 Mono<Map.Entry> 完成后，将结果收集到 Map 中
        return Flux.fromIterable(dependencyMonos)
                .flatMap(mono -> mono) // 触发每个 Mono<Map.Entry> 的执行
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .doOnSuccess(results -> log.debug(
                        "[RequestId: {}] DAG '{}': 节点 '{}' 的所有 {} 个依赖的 NodeResult 已就绪。", // 修改日志，不再说“解析完成”而是“NodeResult 已就绪”
                        requestId, dagName, nodeName, results.size()))
                .doOnError(e -> log.error(
                        "[RequestId: {}] DAG '{}': 节点 '{}' 获取依赖 NodeResult 失败: {}", // 修改日志
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
            String dependentNodeName, // 请求此依赖的节点名称
            String requestId) {

        String depName = dep.getName(); // 依赖的节点名称
        Class<R> requiredPayloadType = (Class<R>) dep.getRequiredType(); // 下游节点期望的 Payload 类型
        String dagName = dagDefinition.getDagName();

        log.trace("[RequestId: {}] DAG '{}': 节点 '{}' 开始解析依赖 '{}' (期望 Payload: {})",
                requestId, dagName, dependentNodeName, depName, requiredPayloadType.getSimpleName());

        // 获取依赖节点的 Mono<NodeResult<C, R, ?>>
        // 这个 Mono 完成时，表示 NodeResult 对象已创建（可能包含活动流）
        Mono<NodeResult<C, R, ?>> dependencyResultMono = getNodeExecutionMono(
                depName, requiredPayloadType, context, cache, dagDefinition, requestId);

        // 当依赖节点的 Mono<NodeResult> 完成时，处理结果
        return dependencyResultMono
                .map(result -> { // 使用 map 直接转换 NodeResult 为 Map.Entry
                    // 检查依赖节点本身是否执行失败
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

    // handleDependencyResult 方法已被移除

    // waitForDependencyEvents 方法已被移除

    // handleDependencyEventStreamError 方法已被移除

    /**
     * 创建依赖名称到其 NodeResult 的映射条目。
     * 输入 NodeResult<C, R, ?>，输出 Map.Entry<String, NodeResult<C, ?, ?>>
     */
    private <C, R> Map.Entry<String, NodeResult<C, ?, ?>> createMapEntry(String depName, NodeResult<C, R, ?> nodeResult) {
        // NodeResult<C, R, ?> 可以安全地赋值给 NodeResult<C, ?, ?>
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
        log.error("[RequestId: {}] DAG '{}': 获取节点 '{}' 的依赖 '{}' 的 NodeResult 失败: {}", // 修改日志
                requestId, dagName, dependentNodeName, depName, e.getMessage(), e);
        // 将错误传播下去，会导致整个依赖解析失败 (collectMap 会失败)
        return Mono.error(e);
    }

    /**
     * 内部方法：实际执行节点逻辑。
     * 返回 Mono<NodeResult<C, P, T>>，其中 T 是由节点决定的具体事件类型。
     */
    private <C, P, T> Mono<NodeResult<C, P, T>> executeNodeInternal(
            DagNode<C, P, T> node,
            C context,
            Map<String, NodeResult<C, ?, ?>> dependencyResults, // 依赖结果是通配符事件类型
            Duration timeout,
            String requestId,
            String dagName) {

        String nodeName = node.getName();
        Class<P> expectedPayloadType = node.getPayloadType(); // 节点声明的 Payload 类型
        // T 是节点声明的 Event 类型，由 node.execute 决定

        return Mono.defer(() -> {
                    log.info("[RequestId: {}] DAG '{}': 开始执行节点 '{}' (Payload: {}, Impl: {}) 逻辑...",
                            requestId, dagName, nodeName, expectedPayloadType.getSimpleName(), node.getClass().getSimpleName());
                    // 调用节点的 execute 方法，它应该返回 Mono<NodeResult<C, P, T>>
                    // 如果是流式节点，这个 Mono 会很快完成并返回包含流引用的 NodeResult
                    // 如果是聚合节点，这个 Mono 会在聚合完成后才返回 NodeResult
                    return node.execute(context, dependencyResults);
                })
                .subscribeOn(nodeExecutionScheduler) // 在指定的调度器上执行节点逻辑
                .timeout(timeout) // 应用节点执行超时 (针对 node.execute() 返回的 Mono)
                .doOnSuccess(result -> validateAndLogResult(result, expectedPayloadType, nodeName, dagName, requestId))
                .doOnError(error -> { // 捕获同步异常或 Mono.error
                    if (!(error instanceof TimeoutException)) { // 超时异常由 onErrorResume 处理
                        log.error(
                                "[RequestId: {}] DAG '{}': 节点 '{}' 执行期间失败 (非超时): {}",
                                requestId, dagName, nodeName, error.getMessage(), error);
                    }
                    // 注意：如果 node.execute() 返回的 Mono 内部的流发生错误，这里的 doOnError 可能不会捕获到，
                    // 错误将在流的订阅者处被观察到。
                })
                .onErrorResume(error -> handleExecutionError( // 调用 handleExecutionError
                        error,
                        context,
                        node,      // <--- 传递 node 实例
                        timeout,
                        requestId,
                        dagName    // <--- 传递 dagName
                ));
    }

    /**
     * 验证节点返回的 NodeResult 并记录日志。
     */
    private <C, P, T> void validateAndLogResult(
            NodeResult<C, P, T> result, // 节点实际返回的结果
            Class<P> expectedPayloadType, // 节点声明的 Payload 类型
            String nodeName,
            String dagName,
            String requestId) {

        // 验证 Payload 类型是否匹配
        // 注意：对于流式节点，Payload 可能为 null 或 Optional.empty()，需要 NodeResult 内部处理好 getPayloadType()
        if (result.getPayloadType() == null) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 返回的 NodeResult 的 Payload 类型为 null!",
                    requestId, dagName, nodeName);
            // 根据你的设计决定是否允许 payloadType 为 null
        } else if (!expectedPayloadType.equals(result.getPayloadType())) {
            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 返回的 NodeResult 的 Payload 类型 ({}) 与其声明的 Payload 类型 ({}) 不匹配! 这可能导致下游类型转换错误。",
                    requestId, dagName, nodeName,
                    result.getPayloadType().getSimpleName(),
                    expectedPayloadType.getSimpleName());
            // 考虑是否应该抛出异常
        }

        if (result.isSuccess()) {
            // 检查事件 Flux 是否存在且非空 (对于流式节点，这通常是真的)
            boolean hasEvents = result.getEvents() != null && result.getEvents() != Flux.<Event<T>>empty(); // 使用通配符检查 empty 单例
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
     * 处理节点执行过程中发生的错误（超时、异常、Mono.error）。
     * 返回 Mono<NodeResult<C, P, T>>，其中 T 是节点声明的事件类型。
     */
    private <C, P, T> Mono<NodeResult<C, P, T>> handleExecutionError(
            Throwable error,
            C context,
            DagNode<C, P, T> node, // <--- 接收 node 实例
            Duration timeout,
            String requestId,
            String dagName) {      // <--- 接收 dagName

        String nodeName = node.getName();
        Class<P> expectedPayloadType = node.getPayloadType(); // 从 node 获取 Payload 类型
        Class<T> expectedEventType = node.getEventType();   // <--- 从 node 获取 Event 类型

        Throwable capturedError;
        if (error instanceof TimeoutException) {
            String timeoutMsg = String.format("节点 '%s' 在 DAG '%s' 中执行 Mono<NodeResult> 超时 (超过 %s)", nodeName, dagName, timeout);
            log.error("[RequestId: {}] {}", requestId, timeoutMsg);
            capturedError = new TimeoutException(timeoutMsg);
            capturedError.initCause(error);
        } else {
            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 执行 Mono<NodeResult> 时发生未捕获异常: {}",
                    requestId, dagName, nodeName, error.getMessage(), error);
            capturedError = error;
        }

        // 创建失败的 NodeResult，现在可以提供 eventType
        NodeResult<C, P, T> failureResult = NodeResult.failure(
                context,
                capturedError,
                expectedPayloadType, // 使用节点声明的 Payload 类型
                expectedEventType    // <--- 使用节点声明的 Event 类型
        );
        return Mono.just(failureResult);
    }
}
