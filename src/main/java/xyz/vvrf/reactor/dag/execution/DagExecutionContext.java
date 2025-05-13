package xyz.vvrf.reactor.dag.execution;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.ErrorHandlingStrategy;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 封装单次 DAG 执行的上下文和运行时状态。
 * 每个 {@link StandardDagEngine#execute(Object, DagDefinition, String)} 调用都会创建一个此类的实例。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen
 */
@Slf4j
@Getter
public class DagExecutionContext<C> {

    private final String requestId;
    private final DagDefinition<C> dagDefinition;
    private final C initialContext;
    private final Instant dagStartTime;

    private final Map<String, Mono<NodeResult<C, ?>>> nodeExecutionMonos = new ConcurrentHashMap<>();
    private final Map<String, NodeResult<C, ?>> completedResults = new ConcurrentHashMap<>();
    private final AtomicBoolean failFastTriggered = new AtomicBoolean(false);
    private final AtomicBoolean overallSuccess = new AtomicBoolean(true);
    private final AtomicInteger completedNodeCounter = new AtomicInteger(0);

    private final String dagName;
    private final ErrorHandlingStrategy errorStrategy;
    private final Set<String> allNodeNames;
    private final int totalNodes;
    private final String contextTypeName;

    public DagExecutionContext(C initialContext, DagDefinition<C> dagDefinition, String rawRequestId) {
        this.initialContext = initialContext;
        this.dagDefinition = dagDefinition;
        this.dagStartTime = Instant.now();

        this.requestId = (rawRequestId != null && !rawRequestId.trim().isEmpty())
                ? rawRequestId
                : "dag-req-" + UUID.randomUUID().toString().substring(0, 8);

        // 初始化派生字段
        this.dagName = dagDefinition.getDagName();
        this.errorStrategy = dagDefinition.getErrorHandlingStrategy();
        this.allNodeNames = dagDefinition.getAllNodeInstanceNames();
        this.totalNodes = this.allNodeNames.size();
        this.contextTypeName = dagDefinition.getContextType().getSimpleName();

        log.info("[RequestId: {}][DAG: '{}'][Context: {}] 创建 DagExecutionContext (Strategy: {}, Nodes: {})",
                this.requestId, this.dagName, this.contextTypeName, this.errorStrategy, this.totalNodes);
    }

    /**
     * 原子性地记录一个节点的完成结果，并增加已完成节点计数器。
     *
     * @param instanceName 节点实例名
     * @param result       节点结果
     * @return 如果结果是新记录的，则返回 true；如果该节点的结果已存在，则返回 false。
     */
    public boolean recordCompletedResult(String instanceName, NodeResult<C, ?> result) {
        if (completedResults.putIfAbsent(instanceName, result) == null) {
            int count = completedNodeCounter.incrementAndGet();
            log.debug("[RequestId: {}][DAG: '{}'] Node '{}' completed with status: {}. Recorded result. Progress: {}/{}",
                    requestId, dagName, instanceName, result.getStatus(), count, totalNodes);
            return true;
        } else {
            log.warn("[RequestId: {}][DAG: '{}'] Node '{}' result ALREADY recorded when trying to add status: {}. This might indicate an issue.",
                    requestId, dagName, instanceName, result.getStatus());
            return false;
        }
    }

    /**
     * 尝试触发 FAIL_FAST 模式。
     *
     * @return 如果 FAIL_FAST 是首次被触发，则返回 true。
     */
    public boolean triggerFailFast() {
        if (failFastTriggered.compareAndSet(false, true)) {
            log.warn("[RequestId: {}][DAG: '{}'] Activating FAIL_FAST strategy due to a failure.",
                    requestId, dagName);
            return true;
        }
        return false;
    }

    /**
     * 将 DAG 的总体执行状态标记为失败。
     */
    public void setOverallFailure() {
        overallSuccess.set(false);
    }

    public boolean isFailFastActive() {
        return failFastTriggered.get();
    }

    public boolean isOverallSuccess() {
        return overallSuccess.get();
    }

    public int getCompletedNodeCount() {
        return completedNodeCounter.get();
    }

    /**
     * 获取或创建指定节点实例的执行 Mono。
     * 使用 computeIfAbsent 确保为每个节点实例只创建一个 Mono。
     *
     * @param instanceName 节点实例名
     * @param monoSupplier 用于创建 Mono 的 Supplier (仅当 Mono 不存在时调用)
     * @return 缓存的或新创建的 Mono
     */
    public Mono<NodeResult<C, ?>> getOrCreateNodeMono(String instanceName, Supplier<Mono<NodeResult<C, ?>>> monoSupplier) {
        return nodeExecutionMonos.computeIfAbsent(instanceName, key -> {
            log.debug("[RequestId: {}][DAG: '{}'] Creating execution Mono for node '{}' (this happens only once per node).",
                    requestId, dagName, instanceName);
            return monoSupplier.get();
        });
    }

    /**
     * 清理此执行上下文持有的 Mono 缓存。
     * 通常在 DAG 执行流的 finally 块中调用。
     */
    public void clearExecutionMonoCache() {
        log.debug("[RequestId: {}][DAG: '{}'] Clearing execution mono cache for this context.", requestId, dagName);
        this.nodeExecutionMonos.clear();
    }
}
