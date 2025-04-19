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
 * 标准节点执行器 - 负责执行单个节点并处理依赖关系
 *
 * @author ruifeng.wen
 */
@Slf4j
public class StandardNodeExecutor {

    private final Duration defaultNodeTimeout;
    private final Duration dependencyStreamTimeout;
    private final Scheduler nodeExecutionScheduler;

    /**
     * 创建标准节点执行器
     *
     * @param defaultNodeTimeout 默认节点执行超时时间
     * @param dependencyStreamTimeout 依赖流等待超时时间
     */
    public StandardNodeExecutor(Duration defaultNodeTimeout, Duration dependencyStreamTimeout) {
        this.defaultNodeTimeout = defaultNodeTimeout;
        this.dependencyStreamTimeout = dependencyStreamTimeout;
        this.nodeExecutionScheduler = Schedulers.boundedElastic();
        log.info("StandardNodeExecutor 初始化完成，节点超时: {}, 依赖流超时: {}",
                defaultNodeTimeout, dependencyStreamTimeout);
    }

    public StandardNodeExecutor(Duration defaultNodeTimeout, Duration dependencyStreamTimeout, Scheduler nodeExecutionScheduler) {
        this.defaultNodeTimeout = defaultNodeTimeout;
        this.dependencyStreamTimeout = dependencyStreamTimeout;
        this.nodeExecutionScheduler = nodeExecutionScheduler;
        log.info("StandardNodeExecutor 初始化完成，节点超时: {}, 依赖流超时: {}",
                defaultNodeTimeout, dependencyStreamTimeout);
    }

    /**
     * 获取节点执行的Mono，支持缓存。
     */
    @SuppressWarnings("unchecked")
    public <C, T> Mono<NodeResult<C, T>> getNodeExecutionMono(
            final String nodeName,
            final Class<T> payloadType,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        final String cacheKey = nodeName + "#" + payloadType.getName();

        return Mono.defer(() -> {
            Mono<? extends NodeResult<C, ?>> cachedMono = cache.getIfPresent(cacheKey);

            if (cachedMono == null) {
                return createAndCacheNodeExecutionMono(
                        nodeName, payloadType, context, cache, dagDefinition, requestId, cacheKey);
            } else {
                log.trace("[RequestId: {}] DAG '{}': 缓存命中节点 '{}' (类型 {})",
                        requestId, dagDefinition.getDagName(), nodeName, payloadType.getSimpleName());
                return castCachedMono(cachedMono, cacheKey, requestId, dagDefinition.getDagName());
            }
        });
    }

    /**
     * 创建并缓存节点执行的Mono
     */
    private <C, T> Mono<NodeResult<C, T>> createAndCacheNodeExecutionMono(
            String nodeName,
            Class<T> payloadType,
            C context,
            Cache<String, Mono<? extends NodeResult<C, ?>>> cache,
            DagDefinition<C> dagDefinition,
            String requestId,
            String cacheKey) {

        log.debug("[RequestId: {}] DAG '{}': 缓存未命中，创建节点 '{}' (类型 {}) 执行Mono",
                requestId, dagDefinition.getDagName(), nodeName, payloadType.getSimpleName());

        Mono<? extends NodeResult<C, ?>> newMono = createNodeExecutionMono(
                nodeName, payloadType, context, cache, dagDefinition, requestId);
        cache.put(cacheKey, newMono);

        return castCachedMono(newMono, cacheKey, requestId, dagDefinition.getDagName());
    }

    /**
     * 转换缓存的Mono类型
     */
    @SuppressWarnings("unchecked")
    private <C, T> Mono<NodeResult<C, T>> castCachedMono(
            Mono<? extends NodeResult<C, ?>> cachedMono,
            String cacheKey,
            String requestId,
            String dagName) {

        try {
            return (Mono<NodeResult<C, T>>) cachedMono;
        } catch (ClassCastException e) {
            log.error("[RequestId: {}] DAG '{}': 缓存类型转换错误 for key '{}': {}",
                    requestId, dagName, cacheKey, e.getMessage());
            return Mono.error(new IllegalStateException("缓存类型不匹配: " + cacheKey + ", " + e.getMessage()));
        }
    }

    /**
     * 创建节点执行的Mono (私有辅助方法)
     */
    private <C, T> Mono<NodeResult<C, T>> createNodeExecutionMono(
            final String nodeName,
            final Class<T> payloadType,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?>>> cache,
            final DagDefinition<C> dagDefinition,
            final String requestId) {

        // 获取节点实例
        DagNode<C, T> node = findNode(nodeName, payloadType, dagDefinition, requestId);
        Duration timeout = determineNodeTimeout(node);

        log.debug("[RequestId: {}] DAG '{}': 准备执行节点 '{}' (类型 {})，超时: {}",
                requestId, dagDefinition.getDagName(), nodeName, payloadType.getSimpleName(), timeout);

        // 解析依赖，执行节点，缓存结果
        return resolveDependencies(node, context, cache, dagDefinition, requestId)
                .flatMap(depResults -> executeNode(node, context, depResults, timeout, requestId, dagDefinition.getDagName()))
                .cache();
    }

    /**
     * 查找并返回节点实例
     */
    private <C, T> DagNode<C, T> findNode(
            String nodeName,
            Class<T> payloadType,
            DagDefinition<C> dagDefinition,
            String requestId) {

        return dagDefinition.getNode(nodeName, payloadType)
                .orElseThrow(() -> {
                    Set<Class<?>> availableTypes = dagDefinition.getSupportedOutputTypes(nodeName);
                    String msg = availableTypes.isEmpty()
                            ? String.format("节点 '%s' 在 DAG '%s' 中不存在", nodeName, dagDefinition.getDagName())
                            : String.format("节点 '%s' 在 DAG '%s' 中不支持类型 '%s'，仅支持: %s",
                            nodeName, dagDefinition.getDagName(), payloadType.getSimpleName(),
                            availableTypes.stream().map(Class::getSimpleName).collect(Collectors.toList()));
                    return new IllegalStateException(msg);
                });
    }

    /**
     * 确定节点执行超时时间
     */
    private <C, T> Duration determineNodeTimeout(DagNode<C, T> node) {
        return Optional.ofNullable(node.getExecutionTimeout())
                .filter(d -> !d.isZero() && !d.isNegative())
                .orElse(defaultNodeTimeout);
    }

    /**
     * 解析节点的所有依赖
     */
    private <C, T> Mono<Map<String, NodeResult<C, ?>>> resolveDependencies(
            final DagNode<C, T> node,
            final C context,
            final Cache<String, Mono<? extends NodeResult<C, ?>>> cache,
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
                dependencies.stream().map(DependencyDescriptor::getName).collect(Collectors.toList()));

        // 创建所有依赖的Mono列表
        List<Mono<Map.Entry<String, NodeResult<C, ?>>>> dependencyMonos = dependencies.stream()
                .map(dep -> resolveDependency(dep, context, cache, dagDefinition, nodeName, requestId))
                .collect(Collectors.toList());

        // 并行解析所有依赖
        return Flux.fromIterable(dependencyMonos)
                .flatMap(mono -> mono)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .doOnSuccess(results -> logDependenciesResolved(results, nodeName, dagName, requestId))
                .doOnError(e -> log.error("[RequestId: {}] DAG '{}': 节点 '{}' 依赖解析失败: {}",
                        requestId, dagName, nodeName, e.getMessage(), e));
    }

    /**
     * 记录依赖解析完成的日志
     */
    private <C> void logDependenciesResolved(
            Map<String, NodeResult<C, ?>> results,
            String nodeName,
            String dagName,
            String requestId) {

        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 的所有依赖解析完成 ({} 个)",
                requestId, dagName, nodeName, results.size());
    }

    /**
     * 解析单个依赖并等待其事件流完成
     */
    @SuppressWarnings("unchecked")
    private <C, R> Mono<Map.Entry<String, NodeResult<C, ?>>> resolveDependency(
            DependencyDescriptor dep,
            C context,
            Cache<String, Mono<? extends NodeResult<C, ?>>> cache,
            DagDefinition<C> dagDefinition,
            String dependentNodeName,
            String requestId) {

        String depName = dep.getName();
        Class<R> requiredType = (Class<R>) dep.getRequiredType();

        // 递归调用获取依赖节点的执行结果
        return getNodeExecutionMono(depName, requiredType, context, cache, dagDefinition, requestId)
                .flatMap(result -> handleDependencyResult(result, depName, dependentNodeName, dagDefinition, requestId))
                .map(result -> createMapEntry(depName, result))
                .onErrorResume(e -> handleDependencyResolutionError(e, depName, dependentNodeName, dagDefinition, requestId));
    }

    /**
     * 处理依赖节点的执行结果
     */
    private <C, R> Mono<NodeResult<C, R>> handleDependencyResult(
            NodeResult<C, R> result,
            String depName,
            String dependentNodeName,
            DagDefinition<C> dagDefinition,
            String requestId) {

        String dagName = dagDefinition.getDagName();

        if (result.getError().isPresent()) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 的依赖 '{}' 执行时遇到错误，跳过其事件流等待: {}",
                    requestId, dagName, dependentNodeName, depName, result.getError().get().getMessage());
            return Mono.just(result);
        }

        log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 等待依赖 '{}' 的事件流完成...",
                requestId, dagName, dependentNodeName, depName);

        return waitForDependencyEvents(result, depName, dependentNodeName, dagName, requestId);
    }

    /**
     * 等待依赖节点的事件流完成
     */
    private <C, R> Mono<NodeResult<C, R>> waitForDependencyEvents(
            NodeResult<C, R> result,
            String depName,
            String dependentNodeName,
            String dagName,
            String requestId) {

        return result.getEvents()
                .doOnError(e -> log.warn(
                        "[RequestId: {}] DAG '{}': 依赖 '{}' 的事件流处理中发生错误: {}",
                        requestId, dagName, depName, e.getMessage()))
                .then(Mono.just(result))
                .timeout(dependencyStreamTimeout, createDependencyTimeoutFallback(result, depName, dependentNodeName, dagName, requestId))
                .onErrorResume(e -> handleDependencyEventStreamError(e, result, depName, dependentNodeName, dagName, requestId));
    }

    /**
     * 创建依赖超时的回退方案
     */
    private <C, R> Mono<NodeResult<C, R>> createDependencyTimeoutFallback(
            NodeResult<C, R> result,
            String depName,
            String dependentNodeName,
            String dagName,
            String requestId) {

        return Mono.defer(() -> {
            log.warn("[RequestId: {}] DAG '{}': 等待依赖 '{}' (为节点 '{}' 服务) 事件流超时 ({}).",
                    requestId, dagName, depName, dependentNodeName, dependencyStreamTimeout);
            return Mono.just(result);
        });
    }

    /**
     * 处理依赖事件流错误
     */
    private <C, R> Mono<NodeResult<C, R>> handleDependencyEventStreamError(
            Throwable e,
            NodeResult<C, R> result,
            String depName,
            String dependentNodeName,
            String dagName,
            String requestId) {

        if (!(e instanceof TimeoutException)) {
            log.error("[RequestId: {}] DAG '{}': 处理依赖 '{}' (为节点 '{}' 服务) 事件流时出错: {}",
                    requestId, dagName, depName, dependentNodeName, e.getMessage(), e);
        }
        return Mono.just(result);
    }

    /**
     * 创建依赖名称与结果的映射条目
     */
    private <C, R> Map.Entry<String, NodeResult<C, ?>> createMapEntry(String depName, NodeResult<C, R> nodeResult) {
        return new AbstractMap.SimpleEntry<>(depName, nodeResult);
    }

    /**
     * 处理依赖解析错误
     */
    private <C> Mono<Map.Entry<String, NodeResult<C, ?>>> handleDependencyResolutionError(
            Throwable e,
            String depName,
            String dependentNodeName,
            DagDefinition<C> dagDefinition,
            String requestId) {

        String dagName = dagDefinition.getDagName();
        log.error("[RequestId: {}] DAG '{}': 解析节点 '{}' 的依赖 '{}' 失败: {}",
                requestId, dagName, dependentNodeName, depName, e.getMessage(), e);
        return Mono.error(e);
    }

    /**
     * 执行节点逻辑
     */
    private <C, T> Mono<NodeResult<C, T>> executeNode(
            DagNode<C, T> node,
            C context,
            Map<String, NodeResult<C, ?>> depResults,
            Duration timeout,
            String requestId,
            String dagName) {

        String nodeName = node.getName();
        Class<T> expectedPayloadType = node.getPayloadType();

        return Mono.defer(() -> {
                    log.debug("[RequestId: {}] DAG '{}': 开始执行节点 '{}' 逻辑...", requestId, dagName, nodeName);
                    return node.execute(context, depResults);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .timeout(timeout)
                .doOnSuccess(result -> validateAndLogResult(result, expectedPayloadType, nodeName, dagName, requestId))
                .onErrorResume(error -> handleExecutionError(error, context, nodeName, dagName, timeout, expectedPayloadType, requestId));
    }

    /**
     * 验证并记录节点执行结果
     */
    private <C, T> void validateAndLogResult(
            NodeResult<C, T> result,
            Class<T> expectedPayloadType,
            String nodeName,
            String dagName,
            String requestId) {

        if (!expectedPayloadType.equals(result.getResultType())) {
            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 返回的 NodeResult 类型 ({}) 与其声明的负载类型 ({}) 不匹配!",
                    requestId, dagName, nodeName,
                    result.getResultType().getSimpleName(),
                    expectedPayloadType.getSimpleName());
        }

        if (result.getError().isPresent()) {
            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行完成，但返回了错误: {}",
                    requestId, dagName, nodeName, result.getError().get().getMessage());
        } else {
            log.info("[RequestId: {}] DAG '{}': 节点 '{}' 执行成功", requestId, dagName, nodeName);
        }
    }

    /**
     * 处理节点执行错误
     */
    private <C, T> Mono<NodeResult<C, T>> handleExecutionError(
            Throwable error,
            C context,
            String nodeName,
            String dagName,
            Duration timeout,
            Class<T> expectedPayloadType,
            String requestId) {

        Throwable capturedError = error;
        if (error instanceof TimeoutException) {
            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 执行超时 ({})",
                    requestId, dagName, nodeName, timeout);
            capturedError = new TimeoutException(
                    String.format("节点 '%s' 在 DAG '%s' 中执行超时 (%s)", nodeName, dagName, timeout));
        } else {
            log.error("[RequestId: {}] DAG '{}': 节点 '{}' 执行时发生未捕获异常: {}",
                    requestId, dagName, nodeName, error.getMessage(), error);
        }

        return Mono.just(new NodeResult<>(
                context,
                Flux.empty(),
                capturedError,
                expectedPayloadType
        ));
    }
}