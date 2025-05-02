// [文件名称]: DagNodeDefinition.java
package xyz.vvrf.reactor.dag.core;

import lombok.Getter;
import reactor.util.retry.Retry;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
// 移除 Map 和 BiPredicate 相关导入

/**
 * 代表在 DAG 中配置好的一个节点实例。
 * 它关联了一个具体的 NodeLogic 实现，并定义了该实例在 DAG 中的名称、入边（依赖及条件）、
 * 超时和重试策略。
 * 节点产生的数据（用于其他节点消费）必须写入共享的 Context 中。
 *
 * @param <C> 上下文类型
 * @param <T> 该节点实例产生的事件类型 (由关联的 NodeLogic 决定)，主要用于最终的 DAG 输出流。
 */
public final class DagNodeDefinition<C, T> {

    @Getter
    private final String nodeName; // 在 DAG 中唯一的名称
    @Getter
    private final NodeLogic<C, T> nodeLogic; // 关联的业务逻辑实现
    /**
     * -- GETTER --
     *  获取定义指向此节点的入边列表。
     *  每条边包含依赖的源节点名称和执行条件。
     *
     * @return 不可变的入边定义列表。
     */
    @Getter
    private final List<EdgeDefinition<C>> incomingEdges; // 定义指向此节点的入边（依赖+条件）
    private final Retry retrySpec; // 此实例的重试策略 (覆盖 NodeLogic 默认)
    private final Duration executionTimeout; // 此实例的超时 (覆盖 NodeLogic 默认)
    // 移除了 shouldExecutePredicate

    // 构造函数通常由 Builder 调用
    public DagNodeDefinition(
            String nodeName,
            NodeLogic<C, T> nodeLogic,
            List<EdgeDefinition<C>> incomingEdges, // 接收边定义列表
            Retry retrySpec, // 可以为 null
            Duration executionTimeout // 可以为 null
            // 移除了 shouldExecutePredicate 参数
    ) {
        this.nodeName = Objects.requireNonNull(nodeName, "节点名称不能为空");
        this.nodeLogic = Objects.requireNonNull(nodeLogic, "NodeLogic 不能为空");
        // 确保传入的列表是不可变的或创建一个不可变副本
        this.incomingEdges = Collections.unmodifiableList(
                Objects.requireNonNull(incomingEdges, "入边定义列表不能为空 (可以是空列表)")
        );
        this.retrySpec = retrySpec; // 允许为 null
        this.executionTimeout = executionTimeout; // 允许为 null

        if (nodeName.trim().isEmpty()) {
            throw new IllegalArgumentException("节点名称不能为空白");
        }
    }

    /**
     * 获取此节点所有直接依赖的节点名称列表。
     *
     * @return 依赖的节点名称列表。
     */
    public List<String> getDependencyNames() {
        return incomingEdges.stream()
                .map(EdgeDefinition::getDependencyNodeName)
                .distinct() // 可能有多条边来自同一依赖，去重
                .collect(Collectors.toList());
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

    // 移除了 shouldExecute 方法
    // public boolean shouldExecute(C context, Map<String, NodeResult<C, ?>> dependencyResults) { ... }


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
        // 在 toString 中包含边的信息可能过于冗长，只显示依赖名称
        return "DagNodeDefinition{" +
                "nodeName='" + nodeName + '\'' +
                ", logic=" + nodeLogic.getLogicIdentifier() +
                ", dependencies=" + getDependencyNames() + // 使用 getDependencyNames()
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
        // 主要基于名称和逻辑标识符判断，不比较边定义细节
        return nodeName.equals(that.nodeName) &&
                nodeLogic.getLogicIdentifier().equals(that.nodeLogic.getLogicIdentifier());
    }

    @Override
    public int hashCode() {
        // 不包含边定义细节
        return Objects.hash(nodeName, nodeLogic.getLogicIdentifier());
    }
}
