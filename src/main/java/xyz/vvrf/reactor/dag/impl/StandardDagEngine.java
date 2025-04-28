package xyz.vvrf.reactor.dag.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import xyz.vvrf.reactor.dag.core.*; // 引入所有 core 包下的类

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 标准DAG执行引擎 - 负责协调节点执行并合并事件流。
 * **采用并行执行策略**：节点在其所有直接执行前驱完成后立即开始执行。
 * 利用 Reactor 的 Mono 和缓存机制实现高效并发。
 *
 * @author ruifeng.wen (Refactored by Devin for Parallel Execution)
 */
@Slf4j
public class StandardDagEngine {

    private final StandardNodeExecutor nodeExecutor;
    private final Duration cacheTtl;

    public StandardDagEngine(StandardNodeExecutor nodeExecutor, Duration cacheTtl) {
        this.nodeExecutor = Objects.requireNonNull(nodeExecutor, "NodeExecutor 不能为空");
        this.cacheTtl = Objects.requireNonNull(cacheTtl, "Cache TTL 不能为空");
        if (cacheTtl.isNegative() || cacheTtl.isZero()) {
            log.warn("StandardDagEngine 配置的 cacheTtl <= 0，节点执行 Mono 缓存将被禁用或立即过期: {}", cacheTtl);
        }
        log.info("StandardDagEngine (Parallel) 初始化完成，节点执行 Mono 缓存TTL: {}", this.cacheTtl);
    }

    /**
     * 执行指定 DAG 定义并返回合并后的事件流。
     * 采用并行执行策略。
     *
     * @param <C>            上下文类型
     * @param initialContext 初始上下文对象
     * @param requestId      请求的唯一标识符 (可选, 为 null 则自动生成)，用于日志和追踪
     * @param dagDefinition  要执行的 DAG 的定义 (必须已初始化)
     * @return 合并所有成功节点事件流的 Flux<Event<?>>
     * @throws IllegalStateException 如果 DAG 定义未初始化
     */
    public <C> Flux<Event<?>> execute(
            final C initialContext,
            final String requestId,
            final DagDefinition<C> dagDefinition
    ) {
        final String dagName = dagDefinition.getDagName();
        final String actualRequestId = (requestId != null && !requestId.trim().isEmpty()) ? requestId : generateRequestId();

        if (!dagDefinition.isInitialized()) {
            log.error("[RequestId: {}] DAG '{}' (上下文类型: {}) 尚未初始化，无法执行。",
                    actualRequestId, dagName, dagDefinition.getContextType().getSimpleName());
            throw new IllegalStateException(String.format("DAG '%s' is not initialized.", dagName));
        }

        log.info("[RequestId: {}] 开始并行执行 DAG '{}' (上下文类型: {})",
                actualRequestId, dagName, dagDefinition.getContextType().getSimpleName());

        // 请求级缓存，存储 Mono<NodeResult>
        final Cache<String, Mono<NodeResult<C, ?, ?>>> requestMonoCache = createRequestMonoCache(actualRequestId, dagName);
        // 用于存储已完成节点结果的并发 Map
        final ConcurrentHashMap<String, NodeResult<C, ?, ?>> completedResultsMap = new ConcurrentHashMap<>();

        Collection<DagNode<C, ?, ?>> allNodes = dagDefinition.getAllNodes();
        if (allNodes.isEmpty()) {
            log.info("[RequestId: {}] DAG '{}' 为空，直接完成。", actualRequestId, dagName);
            return Flux.empty();
        }

        // --- 核心变化：为所有节点创建或获取执行 Mono ---
        List<Mono<NodeResult<C, ?, ?>>> allNodeMonos = allNodes.stream()
                .map(node -> getOrCreateNodeExecutionMono(
                        node.getName(),
                        initialContext,
                        requestMonoCache,
                        completedResultsMap, // 传递并发 Map
                        dagDefinition,
                        actualRequestId
                ))
                .collect(Collectors.toList());

        // --- 触发执行并合并事件流 ---
        // 使用 Flux.merge 并发订阅所有节点的 Mono。
        // flatMapMany 从每个完成的 Mono 中提取事件流 (如果成功)。
        return Flux.fromIterable(allNodeMonos)
                .flatMap(nodeMono -> nodeMono // 订阅每个节点的 Mono
                        .flatMapMany(result -> { // 当节点 Mono 完成时
                            if (result.isSuccess()) {
                                // 成功，提取事件流
                                @SuppressWarnings("unchecked") // 类型安全，来自 NodeResult
                                Flux<Event<?>> events = (Flux<Event<?>>) (Flux<? extends Event<?>>) result.getEvents();
                                return events
                                        .doOnError(e -> log.error(
                                                "[RequestId: {}] DAG '{}': 节点 '{}' 的事件流处理中发生错误: {}",
                                                actualRequestId, dagName, result.getNodeName(), e.getMessage(), e
                                        ))
                                        .onErrorResume(e -> {
                                            log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 事件流错误被忽略，继续执行。", actualRequestId, dagName, result.getNodeName());
                                            return Flux.empty(); // 忽略事件流错误
                                        });
                            } else {
                                // 失败或跳过，无事件流
                                return Flux.empty();
                            }
                        })
                        // 如果节点 Mono 本身失败 (例如配置错误或依赖等待失败)，也返回空 Flux
                        .onErrorResume(error -> {
                            log.error("[RequestId: {}] DAG '{}': 获取节点结果或处理事件时发生意外错误 (可能在节点执行Mono内部): {}",
                                    actualRequestId, dagName, error.getMessage(), error);
                            return Flux.empty();
                        })
                )
                .doOnSubscribe(s -> log.debug("[RequestId: {}] DAG '{}' 最终事件流已订阅，开始并行执行并接收所有节点事件。", actualRequestId, dagName))
                .doOnNext(event -> log.trace("[RequestId: {}] DAG '{}' 最终流收到事件: {}", actualRequestId, dagName, event))
                .doOnComplete(() -> log.info("[RequestId: {}] DAG '{}' 所有节点处理完成，最终事件流处理完成。", actualRequestId, dagName))
                .doOnError(e -> log.error("[RequestId: {}] DAG '{}' 合并或处理事件流时发生错误: {}",
                        actualRequestId, dagName, e.getMessage(), e))
                .doFinally(signal -> cleanupCache(requestMonoCache, actualRequestId, dagName, signal)); // 清理 Mono 缓存
    }

    /**
     * 获取或创建指定节点的执行 Mono。
     * 这个 Mono 内部会处理依赖等待、执行和结果缓存。
     *
     * @param nodeName            节点名称
     * @param context             上下文
     * @param monoCache           请求级 Mono 缓存
     * @param completedResultsMap 用于存储/读取已完成结果的并发 Map
     * @param dagDefinition       DAG 定义
     * @param requestId           请求 ID
     * @return 该节点执行的 Mono<NodeResult>，会被缓存
     */
    private <C> Mono<NodeResult<C, ?, ?>> getOrCreateNodeExecutionMono(
            final String nodeName,
            final C context,
            final Cache<String, Mono<NodeResult<C, ?, ?>>> monoCache,
            final ConcurrentHashMap<String, NodeResult<C, ?, ?>> completedResultsMap,
            final DagDefinition<C> dagDefinition,
            final String requestId
    ) {
        // 使用 Caffeine 的 computeIfAbsent 原子性地获取或创建 Mono
        return monoCache.get(nodeName, key -> {
            log.debug("[RequestId: {}] DAG '{}': 缓存未命中，创建节点 '{}' 的并行执行 Mono",
                    requestId, dagDefinition.getDagName(), nodeName);

            // 使用 Mono.defer 延迟执行，确保每次订阅（如果缓存未命中）都执行查找和依赖解析
            return Mono.defer(() -> {
                        // 1. 获取节点实例
                        final DagNode<C, ?, ?> node;
                        try {
                            node = dagDefinition.getNodeAnyType(nodeName)
                                    .orElseThrow(() -> new StandardNodeExecutor.NodeConfigurationException( // 使用 Executor 内定义的异常
                                            String.format("Node '%s' not found in DAG definition.", nodeName), null, nodeName, dagDefinition.getDagName()));
                        } catch (StandardNodeExecutor.NodeConfigurationException e) {
                            log.error("[RequestId: {}] DAG '{}': 配置错误 - 节点 '{}' 未找到。", requestId, dagDefinition.getDagName(), nodeName, e);
                            // 创建一个表示配置错误的失败结果并放入 Map
                            NodeResult<C, ?, ?> configFailureResult = createConfigFailureResult(context, e, nodeName, dagDefinition);
                            completedResultsMap.put(nodeName, configFailureResult);
                            return Mono.just(configFailureResult); // 返回包含结果的 Mono
                        } catch (Exception e) {
                            log.error("[RequestId: {}] DAG '{}': 查找节点 '{}' 时发生意外错误。", requestId, dagDefinition.getDagName(), nodeName, e);
                            NodeResult<C, ?, ?> findFailureResult = createConfigFailureResult(context, e, nodeName, dagDefinition);
                            completedResultsMap.put(nodeName, findFailureResult);
                            return Mono.just(findFailureResult);
                        }

                        // 2. 获取直接执行前驱
                        Set<String> predecessorNames = dagDefinition.getExecutionPredecessors(nodeName);

                        // 3. 递归获取前驱节点的执行 Mono
                        List<Mono<NodeResult<C, ?, ?>>> predecessorMonos = Collections.emptyList();
                        if (!predecessorNames.isEmpty()) {
                            predecessorMonos = predecessorNames.stream()
                                    .map(predName -> getOrCreateNodeExecutionMono( // 递归调用
                                            predName,
                                            context,
                                            monoCache,
                                            completedResultsMap,
                                            dagDefinition,
                                            requestId
                                    ))
                                    .collect(Collectors.toList());
                        }

                        // 4. 创建等待前驱完成的 Mono
                        // 使用 whenDelayError 确保即使有前驱失败，也会等待所有前驱完成（或失败）
                        Mono<Void> depsCompletionMono = predecessorMonos.isEmpty()
                                ? Mono.empty()
                                : Mono.whenDelayError(predecessorMonos).then();

                        // 5. 定义核心执行逻辑 Mono (在前驱完成后执行)
                        Mono<NodeResult<C, ?, ?>> coreExecutionMono = depsCompletionMono.then(
                                // 使用 Mono.defer 确保在依赖完成后才创建 Accessor 和执行
                                Mono.defer(() -> {
                                    log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 的前驱处理完成，准备执行核心逻辑。",
                                            requestId, dagDefinition.getDagName(), nodeName);

                                    // --- 关键: 构建 InputDependencyAccessor ---
                                    // 此时，所有直接前驱（以及它们的依赖）理论上应该在 completedResultsMap 中有结果了
                                    // 创建 Map 的快照以传递给 Accessor，避免并发修改问题
                                    Map<String, NodeResult<C, ?, ?>> currentResultsSnapshot = new HashMap<>(completedResultsMap);
                                    Map<InputRequirement<?>, String> nodeInputMappings = dagDefinition.getInputMappingForNode(nodeName);
                                    InputDependencyAccessor<C> accessor = new DefaultInputDependencyAccessor<>(
                                            currentResultsSnapshot, // 使用快照
                                            nodeInputMappings,
                                            nodeName,
                                            dagDefinition.getDagName());

                                    // --- 调用 NodeExecutor 执行核心逻辑 ---
                                    // 传递 accessor
                                    return nodeExecutor.executeNodeCoreLogic(
                                            node,
                                            context,
                                            accessor, // 传递构建好的 accessor
                                            dagDefinition.getDagName(),
                                            requestId
                                    );
                                })
                        );

                        // 6. 处理结果：更新 completedResultsMap 并返回 Mono
                        return coreExecutionMono
                                .doOnSuccess(result -> {
                                    log.debug("[RequestId: {}] DAG '{}': 节点 '{}' 核心逻辑 Mono 成功完成，结果状态: {}",
                                            requestId, dagDefinition.getDagName(), nodeName, result.getStatus());
                                    // 原子性地放入结果 Map
                                    completedResultsMap.put(nodeName, result);
                                })
                                .doOnError(error -> {
                                    // 如果 coreExecutionMono 内部发生错误 (例如 execute 失败且未被 onErrorResume 捕获)
                                    log.error("[RequestId: {}] DAG '{}': 节点 '{}' 核心逻辑 Mono 失败: {}",
                                            requestId, dagDefinition.getDagName(), nodeName, error.getMessage(), error);
                                    // 创建失败结果并放入 Map (如果尚未存在)
                                    NodeResult<C, ?, ?> failureResult = NodeResult.createFailureResult(context, error, node); // 使用 Executor 的方法
                                    completedResultsMap.putIfAbsent(nodeName, failureResult);
                                })
                                // 确保即使发生错误，也返回一个包含结果的 Mono，并将结果放入 Map
                                .onErrorResume(error -> {
                                    log.warn("[RequestId: {}] DAG '{}': 节点 '{}' 执行链中捕获到错误，确保生成失败结果。",
                                            requestId, dagDefinition.getDagName(), nodeName);
                                    // 再次尝试创建失败结果并放入 Map
                                    NodeResult<C, ?, ?> failureResult = completedResultsMap.computeIfAbsent(nodeName, k ->
                                            NodeResult.createFailureResult(context, error, node)); // 使用 Executor 的方法
                                    return Mono.just(failureResult);
                                });
                    })
                    // --- 关键: 缓存整个 Mono ---
                    .cache(); // 使用 Reactor 的 cache 操作符
        });
    }

    // 辅助方法：为配置错误创建 NodeResult
    // 需要知道节点的预期类型，但此时可能无法获取，因此创建通用的 Object/Object 类型结果
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <C> NodeResult<C, ?, ?> createConfigFailureResult(C context, Throwable error, String nodeName, DagDefinition<C> dagDefinition) {
        // 尝试获取节点信息以确定类型，如果失败则使用 Object
        Class<?> payloadType = Object.class;
        Class<?> eventType = Object.class;
        Optional<DagNode<C, ?, ?>> nodeOpt = dagDefinition.getNodeAnyType(nodeName);
        if (nodeOpt.isPresent()) {
            payloadType = nodeOpt.get().getPayloadType();
            eventType = nodeOpt.get().getEventType();
        } else {
            log.warn("[RequestId: unknown] DAG '{}': 无法找到节点 '{}' 来确定配置失败结果的类型，将使用 Object/Object。", dagDefinition.getDagName(), nodeName);
        }

        return NodeResult.failureForNode(context, error, payloadType, eventType, nodeName);
    }


    private String generateRequestId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    // 创建请求级 Mono 缓存 (与之前类似)
    private <C> Cache<String, Mono<NodeResult<C, ?, ?>>> createRequestMonoCache(String requestId, String dagName) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(1000); // 可配置

        if (!cacheTtl.isNegative() && !cacheTtl.isZero()) {
            builder.expireAfterWrite(this.cacheTtl);
            log.debug("[RequestId: {}] DAG '{}': 创建请求级节点执行 Mono 缓存，TTL: {}", requestId, dagName, cacheTtl);
        } else {
            log.debug("[RequestId: {}] DAG '{}': 创建请求级节点执行 Mono 缓存，TTL 无效，缓存将立即过期或不生效", requestId, dagName);
            builder.expireAfterWrite(Duration.ZERO); // 立即过期
        }
        @SuppressWarnings("unchecked")
        Cache<String, Mono<NodeResult<C, ?, ?>>> builtCache = (Cache<String, Mono<NodeResult<C, ?, ?>>>)(Cache<?, ?>)builder.build();
        return builtCache;
    }

    // 清理 Mono 缓存 (与之前类似)
    private <C> void cleanupCache(
            Cache<String, Mono<NodeResult<C, ?, ?>>> requestMonoCache,
            String requestId,
            String dagName,
            SignalType signal) {

        long cacheSize = requestMonoCache.estimatedSize();
        if (cacheSize > 0) {
            log.debug("[RequestId: {}] DAG '{}' 最终事件流终止 (信号: {}), 清理节点执行 Mono 缓存 (大小: {}).",
                    requestId, dagName, signal, cacheSize);
            requestMonoCache.invalidateAll();
            requestMonoCache.cleanUp();
            log.debug("[RequestId: {}] DAG '{}' 节点执行 Mono 缓存已清理.", requestId, dagName);
        } else {
            log.trace("[RequestId: {}] DAG '{}' 最终事件流终止 (信号: {}), 节点执行 Mono 缓存为空，无需清理.", requestId, dagName, signal);
        }
    }
}
