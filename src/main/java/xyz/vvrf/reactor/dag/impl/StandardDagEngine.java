// file: impl/StandardDagEngine.java
package xyz.vvrf.reactor.dag.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import xyz.vvrf.reactor.dag.core.*; // 引入核心接口

import java.time.Duration; // 保留 Duration 导入，可能其他地方仍需
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 标准DAG执行引擎 - 负责协调节点实例执行并合并事件流。
 * 支持错误处理策略 (目前仅 FAIL_FAST)。
 * 缓存机制已简化：移除请求级缓存的 TTL 配置。
 *
 * @author ruifeng.wen (Refactored & Modified)
 */
@Slf4j
public class StandardDagEngine {

    private final StandardNodeExecutor nodeExecutor;
    // 移除了 cacheTtl 字段
    // private final Duration cacheTtl;
    private final int concurrencyLevel;

    /**
     * 构造函数更新：不再接收 cacheTtl。
     *
     * @param nodeExecutor     节点执行器
     * @param concurrencyLevel 并发级别
     */
    public StandardDagEngine(StandardNodeExecutor nodeExecutor, int concurrencyLevel) {
        this.nodeExecutor = Objects.requireNonNull(nodeExecutor, "NodeExecutor cannot be null");
        // 移除了 cacheTtl 的检查和赋值
        // this.cacheTtl = Objects.requireNonNull(cacheTtl, "Cache TTL cannot be null");
        if (concurrencyLevel <= 0) {
            throw new IllegalArgumentException("Concurrency level must be positive.");
        }
        this.concurrencyLevel = concurrencyLevel;
        log.info("StandardDagEngine initialized. Concurrency: {}", this.concurrencyLevel);
    }

    // 保留旧构造函数（如果需要向后兼容或有其他用途），但标记为弃用或移除
    // public StandardDagEngine(StandardNodeExecutor nodeExecutor, Duration cacheTtl, int concurrencyLevel) { ... }
    // public StandardDagEngine(StandardNodeExecutor nodeExecutor, Duration cacheTtl) { ... }


    /**
     * 执行指定 DAG 定义并返回合并后的事件流。
     *
     * @param <C>            上下文类型
     * @param initialContext 初始上下文对象
     * @param requestId      请求的唯一标识符 (可选, 为 null 则自动生成)
     * @param dagDefinition  要执行的 DAG 的定义 (必须已通过 Builder 构建)
     * @return 合并所有节点事件流的 Flux<Event<?>>
     * @throws NullPointerException 如果 dagDefinition 为 null
     */
    public <C> Flux<Event<?>> execute(
            final C initialContext,
            final String requestId,
            final DagDefinition<C> dagDefinition // 直接使用接口
    ) {
        Objects.requireNonNull(dagDefinition, "DagDefinition cannot be null");

        final String dagName = dagDefinition.getDagName();
        final String actualRequestId = (requestId != null && !requestId.trim().isEmpty()) ? requestId : generateRequestId();
        final ErrorHandlingStrategy errorStrategy = dagDefinition.getErrorHandlingStrategy();

        log.info("[RequestId: {}] Starting execution of DAG '{}' (Context: {}, Strategy: {})",
                actualRequestId, dagName, dagDefinition.getContextType().getSimpleName(), errorStrategy);

        // 获取拓扑排序后的节点实例名称
        List<String> nodeInstanceNames = dagDefinition.getExecutionOrder();
        if (nodeInstanceNames.isEmpty()) {
            log.info("[RequestId: {}] DAG '{}' has no nodes to execute. Completing.", actualRequestId, dagName);
            return Flux.empty();
        }

        // 为当前请求创建节点结果缓存 (Mono<NodeResult>)
        // Key: instanceName, Value: Mono<NodeResult<C, ?>>
        // 这个缓存用于在单次执行中共享节点的 Mono 实例 (通常是经过 .cache() 的 Mono)
        final Cache<String, Mono<NodeResult<C, ?>>> requestCache = createRequestCache();
        // 用于 FAIL_FAST 策略，标记是否已发生错误
        final AtomicBoolean errorOccurred = new AtomicBoolean(false);

        // 使用 flatMapSequential 保证按拓扑顺序处理节点，同时允许内部并发（如果调度器支持）
        // flatMapSequential 会等待前一个内部 Mono 完成（但不一定成功）再订阅下一个
        Flux<NodeResult<C, ?>> resultsFlux = Flux.fromIterable(nodeInstanceNames)
                .flatMapSequential(instanceName -> {
                    // 如果是 FAIL_FAST 且已出错，则跳过后续所有节点
                    if (errorStrategy == ErrorHandlingStrategy.FAIL_FAST && errorOccurred.get()) {
                        log.debug("[RequestId: {}] DAG '{}': FAIL_FAST active, skipping node '{}' due to previous error.",
                                actualRequestId, dagName, instanceName);
                        // 需要返回一个表示跳过的 Mono，但需要知道节点的 Payload 类型
                        // 从 DagDefinition 获取类型
                        Class<?> payloadType = dagDefinition.getNodeOutputType(instanceName)
                                .orElseThrow(() -> new IllegalStateException("Cannot determine payload type for skipped node " + instanceName));
                        // 注意：这里创建的是即时完成的 Mono，不会放入 requestCache
                        return Mono.just(NodeResult.skipped(initialContext, payloadType)); // 返回跳过结果
                    }

                    // 获取或创建节点的执行 Mono (此方法内部会处理缓存)
                    return nodeExecutor.getNodeExecutionMono(
                            instanceName,
                            initialContext,
                            requestCache, // 传递缓存给执行器
                            dagDefinition,
                            actualRequestId
                    ).doOnNext(result -> { // 检查结果，更新错误标志
                        if (result.isFailure() && errorStrategy == ErrorHandlingStrategy.FAIL_FAST) {
                            if (errorOccurred.compareAndSet(false, true)) {
                                log.warn("[RequestId: {}] DAG '{}': Node '{}' failed. Activating FAIL_FAST.",
                                        actualRequestId, dagName, instanceName);
                            }
                        }
                    }).doOnError(err -> { // 如果 Mono 本身出错 (例如，准备输入时)
                        if (errorStrategy == ErrorHandlingStrategy.FAIL_FAST) {
                            if (errorOccurred.compareAndSet(false, true)) {
                                log.error("[RequestId: {}] DAG '{}': Error during execution setup for node '{}'. Activating FAIL_FAST.",
                                        actualRequestId, dagName, instanceName, err);
                            }
                        }
                        // 让错误继续传播，以便 onErrorResume 处理 (如果顶层有的话)
                        // 或者在这里转换为失败的 NodeResult Mono?
                        // 保持传播，让 flatMapSequential 的错误处理机制接管
                    });
                }, concurrencyLevel); // 并发度应用于 flatMapSequential 的内部订阅

        // 处理结果流：提取事件，处理错误，清理缓存
        return resultsFlux
                .flatMap(result -> { // 从每个结果中提取事件流
                    if (result.isSuccess()) {
                        // result.getEvents() 返回 Flux<Event<?>>
                        return result.getEvents()
                                .doOnError(e -> log.error(
                                        "[RequestId: {}] DAG '{}': Error processing event stream from successful node result (Type: {}): {}",
                                        actualRequestId, dagName, result.getPayloadType().getSimpleName(), e.getMessage(), e))
                                .onErrorResume(e -> Flux.empty()); // 忽略出错的事件流
                    } else {
                        // 失败或跳过的节点不产生事件
                        return Flux.empty();
                    }
                })
                .doOnSubscribe(s -> log.debug("[RequestId: {}] DAG '{}' event stream subscribed.", actualRequestId, dagName))
                .doOnNext(event -> log.trace("[RequestId: {}] DAG '{}' emitting event: {}", actualRequestId, dagName, event))
                .doOnComplete(() -> {
                    if (!errorOccurred.get()) {
                        log.info("[RequestId: {}] DAG '{}' execution completed successfully.", actualRequestId, dagName);
                    } else {
                        log.warn("[RequestId: {}] DAG '{}' execution completed with failures (FAIL_FAST).", actualRequestId, dagName);
                    }
                })
                .doOnError(e -> log.error("[RequestId: {}] DAG '{}' execution failed with an unexpected error in the main stream: {}",
                        actualRequestId, dagName, e.getMessage(), e))
                .doFinally(signal -> cleanupCache(requestCache, actualRequestId, dagName, signal)); // 清理缓存
    }

    // --- Helper Methods ---

    private String generateRequestId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * 创建请求级缓存。
     * 不再设置 TTL，因为缓存生命周期与请求绑定。
     */
    private <C> Cache<String, Mono<NodeResult<C, ?>>> createRequestCache() {
        // 移除 TTL 配置
        Caffeine<Object, Object> builder = Caffeine.newBuilder().maximumSize(1000); // 限制大小防止内存泄漏
        log.debug("Created request cache (no TTL, lifecycle bound to request).");
        return builder.build();
    }

    private <C> void cleanupCache(
            Cache<String, Mono<NodeResult<C, ?>>> requestCache,
            String requestId,
            String dagName,
            SignalType signal) {
        long cacheSize = requestCache.estimatedSize();
        log.debug("[RequestId: {}] DAG '{}' execution finished (Signal: {}). Cleaning up request cache (Size: {}).",
                requestId, dagName, signal, cacheSize);
        requestCache.invalidateAll(); // 使所有缓存条目失效
        requestCache.cleanUp();      // 执行清理操作
        log.debug("[RequestId: {}] DAG '{}' request cache cleaned.", requestId, dagName);
    }
}
