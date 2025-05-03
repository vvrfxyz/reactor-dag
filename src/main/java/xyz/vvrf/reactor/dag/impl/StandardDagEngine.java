// file: impl/StandardDagEngine.java
package xyz.vvrf.reactor.dag.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import xyz.vvrf.reactor.dag.core.*; // 引入核心接口

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 标准DAG执行引擎 - 负责协调节点实例执行并合并事件流。
 * 支持错误处理策略 (目前仅 FAIL_FAST)。
 *
 * @author ruifeng.wen (Refactored)
 */
@Slf4j
public class StandardDagEngine {

    private final StandardNodeExecutor nodeExecutor;
    private final Duration cacheTtl;
    private final int concurrencyLevel;

    // 构造函数保持不变，但日志和内部逻辑会调整
    public StandardDagEngine(StandardNodeExecutor nodeExecutor, Duration cacheTtl, int concurrencyLevel) {
        this.nodeExecutor = Objects.requireNonNull(nodeExecutor, "NodeExecutor cannot be null");
        this.cacheTtl = Objects.requireNonNull(cacheTtl, "Cache TTL cannot be null");
        if (concurrencyLevel <= 0) {
            throw new IllegalArgumentException("Concurrency level must be positive.");
        }
        this.concurrencyLevel = concurrencyLevel;
        // ... [日志] ...
        log.info("StandardDagEngine initialized. Node result cache TTL: {}, Concurrency: {}", this.cacheTtl, this.concurrencyLevel);
    }
    public StandardDagEngine(StandardNodeExecutor nodeExecutor, Duration cacheTtl) {
        this(nodeExecutor, cacheTtl, Math.max(1, Runtime.getRuntime().availableProcessors()));
    }

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
                        return Mono.just(NodeResult.skipped(initialContext, payloadType)); // 返回跳过结果
                    }

                    // 获取或创建节点的执行 Mono
                    return nodeExecutor.getNodeExecutionMono(
                            instanceName,
                            initialContext,
                            requestCache,
                            dagDefinition,
                            actualRequestId
                    ).doOnNext(result -> { // 检查结果，更新错误标志
                        if (result.isFailure() && errorStrategy == ErrorHandlingStrategy.FAIL_FAST) {
                            if (errorOccurred.compareAndSet(false, true)) {
                                log.warn("[RequestId: {}] DAG '{}': Node '{}' failed. Activating FAIL_FAST.",
                                        actualRequestId, dagName, instanceName);
                            }
                        }
                    }).doOnError(err -> { // 如果 Mono 本身出错
                        if (errorStrategy == ErrorHandlingStrategy.FAIL_FAST) {
                            if (errorOccurred.compareAndSet(false, true)) {
                                log.error("[RequestId: {}] DAG '{}': Error during execution of node '{}' mono. Activating FAIL_FAST.",
                                        actualRequestId, dagName, instanceName, err);
                            }
                        }
                        // 让错误继续传播，以便 onErrorResume 处理
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

    // --- Helper Methods (generateRequestId, createRequestCache, cleanupCache) ---
    // 这些方法基本保持不变，但 createRequestCache 的 Value 类型变为 Mono<NodeResult<C, ?>>

    private String generateRequestId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private <C> Cache<String, Mono<NodeResult<C, ?>>> createRequestCache() {
        Caffeine<Object, Object> builder = Caffeine.newBuilder().maximumSize(1000);
        if (!cacheTtl.isNegative() && !cacheTtl.isZero()) {
            builder.expireAfterWrite(this.cacheTtl);
        } else {
            builder.expireAfterWrite(Duration.ZERO); // 禁用或立即过期
        }
        log.debug("Created request cache with TTL: {}", cacheTtl);
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
        requestCache.invalidateAll();
        requestCache.cleanUp();
        log.debug("[RequestId: {}] DAG '{}' request cache cleaned.", requestId, dagName);
    }
}
