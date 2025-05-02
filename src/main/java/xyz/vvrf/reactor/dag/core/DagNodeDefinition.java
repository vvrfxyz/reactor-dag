package xyz.vvrf.reactor.dag.core;

/**
 * reactor-dag
 *
 * @author ruifeng.wen
 * @date 5/2/25
 */

import reactor.util.retry.Retry;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * 代表在 DAG 中配置好的一个节点实例。
 * 它关联了一个具体的 NodeLogic 实现，并定义了该实例在 DAG 中的名称、依赖关系、
 * 超时、重试和执行条件。
 *
 * @param <C> 上下文类型
 * @param <T> 该节点实例产生的事件类型 (由关联的 NodeLogic 决定)
 */
public final class DagNodeDefinition<C, T> {

    private final String nodeName; // 在 DAG 中唯一的名称
    private final NodeLogic<C, T> nodeLogic; // 关联的业务逻辑实现
    private final List<String> dependencyNames; // 依赖的节点名称列表
    private final Retry retrySpec; // 此实例的重试策略 (覆盖 NodeLogic 默认)
    private final Duration executionTimeout; // 此实例的超时 (覆盖 NodeLogic 默认)
    private final java.util.function.BiPredicate<C, DependencyAccessor<C>> shouldExecutePredicate; // 此实例的执行条件

    // 构造函数通常由 Builder 调用
    public DagNodeDefinition(
            String nodeName,
            NodeLogic<C, T> nodeLogic,
            List<String> dependencyNames,
            Retry retrySpec, // 可以为 null
            Duration executionTimeout, // 可以为 null
            java.util.function.BiPredicate<C, DependencyAccessor<C>> shouldExecutePredicate // 可以为 null
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
     * @param context      上下文。
     * @param dependencies 依赖访问器。
     * @return 是否应该执行。
     */
    public boolean shouldExecute(C context, DependencyAccessor<C> dependencies) {
        if (shouldExecutePredicate != null) {
            return shouldExecutePredicate.test(context, dependencies);
        }
        return nodeLogic.shouldExecute(context, dependencies);
    }

    /**
     * 获取此节点实例产生的事件类型。
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
        // 主要基于名称和逻辑标识符判断，因为依赖等可能在不同 DAG 中不同
        return nodeName.equals(that.nodeName) &&
                nodeLogic.getLogicIdentifier().equals(that.nodeLogic.getLogicIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeName, nodeLogic.getLogicIdentifier());
    }
}
