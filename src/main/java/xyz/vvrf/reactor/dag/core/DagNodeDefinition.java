// [文件名称]: DagNodeDefinition.java
package xyz.vvrf.reactor.dag.core;

import reactor.util.retry.Retry;
import java.time.Duration;
import java.util.List;
import java.util.Map; // 引入 Map
import java.util.Objects;
import java.util.function.BiPredicate; // 引入 BiPredicate

/**
 * 代表在 DAG 中配置好的一个节点实例。
 * 它关联了一个具体的 NodeLogic 实现，并定义了该实例在 DAG 中的名称、依赖关系、
 * 超时、重试和执行条件。
 * 节点产生的数据（用于其他节点消费）必须写入共享的 Context 中。
 *
 * @param <C> 上下文类型
 * @param <T> 该节点实例产生的事件类型 (由关联的 NodeLogic 决定)，主要用于最终的 DAG 输出流。
 */
public final class DagNodeDefinition<C, T> {

    private final String nodeName; // 在 DAG 中唯一的名称
    private final NodeLogic<C, T> nodeLogic; // 关联的业务逻辑实现
    private final List<String> dependencyNames; // 依赖的节点名称列表
    private final Retry retrySpec; // 此实例的重试策略 (覆盖 NodeLogic 默认)
    private final Duration executionTimeout; // 此实例的超时 (覆盖 NodeLogic 默认)
    // 执行条件 Predicate，接收 Context 和 依赖节点的结果 Map
    private final BiPredicate<C, Map<String, NodeResult<C, ?>>> shouldExecutePredicate;

    // 构造函数通常由 Builder 调用
    public DagNodeDefinition(
            String nodeName,
            NodeLogic<C, T> nodeLogic,
            List<String> dependencyNames,
            Retry retrySpec, // 可以为 null
            Duration executionTimeout, // 可以为 null
            BiPredicate<C, Map<String, NodeResult<C, ?>>> shouldExecutePredicate // 可以为 null
    ) {
        this.nodeName = Objects.requireNonNull(nodeName, "节点名称不能为空");
        this.nodeLogic = Objects.requireNonNull(nodeLogic, "NodeLogic 不能为空");
        this.dependencyNames = Objects.requireNonNull(dependencyNames, "依赖名称列表不能为空 (可以是空列表)");
        this.retrySpec = retrySpec; // 允许为 null
        this.executionTimeout = executionTimeout; // 允许为 null
        this.shouldExecutePredicate = shouldExecutePredicate; // 允许为 null

        if (nodeName.trim().isEmpty()) {
            throw new IllegalArgumentException("节点名称不能为空白");
        }
    }

    public String getNodeName() {
        return nodeName;
    }

    public NodeLogic<C, T> getNodeLogic() {
        return nodeLogic;
    }

    public List<String> getDependencyNames() {
        // 返回不可变副本或确保原始列表不可变
        return dependencyNames;
    }

    /**
     * 获取最终生效的重试策略。
     * 优先使用本实例配置的，否则使用 NodeLogic 的默认值。
     *
     * @return 生效的 Retry 规范，可能为 null 或 Retry.max(0)。
     */
    public Retry getEffectiveRetrySpec() {
        return (retrySpec != null) ? retrySpec : nodeLogic.getDefaultRetrySpec();
    }

    /**
     * 获取最终生效的执行超时。
     * 优先使用本实例配置的，否则使用 NodeLogic 的默认值。
     *
     * @return 生效的超时 Duration，可能为 null。
     */
    public Duration getEffectiveExecutionTimeout() {
        return (executionTimeout != null) ? executionTimeout : nodeLogic.getDefaultExecutionTimeout();
    }

    /**
     * 判断此节点实例是否应该执行。
     * 如果定义了实例特定的 Predicate，则使用它；否则使用 NodeLogic 的默认行为。
     *
     * @param context           上下文。
     * @param dependencyResults 依赖节点的执行结果 Map (Key: 节点名, Value: NodeResult)。
     * @return 是否应该执行。
     */
    public boolean shouldExecute(C context, Map<String, NodeResult<C, ?>> dependencyResults) {
        if (shouldExecutePredicate != null) {
            // 使用实例特定的 Predicate
            return shouldExecutePredicate.test(context, dependencyResults);
        }
        // 使用 NodeLogic 的默认 Predicate
        return nodeLogic.shouldExecute(context, dependencyResults);
    }

    /**
     * 获取此节点实例产生的事件类型。
     * 这个类型主要用于最终的 DAG 输出事件流。
     *
     * @return 事件类型的 Class 对象。
     */
    public Class<T> getEventType() {
        return nodeLogic.getEventType();
    }

    @Override
    public String toString() {
        return "DagNodeDefinition{" +
                "nodeName='" + nodeName + '\'' +
                ", logic=" + nodeLogic.getLogicIdentifier() +
                ", dependencies=" + dependencyNames +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DagNodeDefinition<?, ?> that = (DagNodeDefinition<?, ?>) o;
        // 主要基于名称和逻辑标识符判断
        return nodeName.equals(that.nodeName) &&
                nodeLogic.getLogicIdentifier().equals(that.nodeLogic.getLogicIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeName, nodeLogic.getLogicIdentifier());
    }
}
