package xyz.vvrf.reactor.dag.core;

import java.util.Objects;

/**
 * DAG 中连接边的定义（不可变数据类）。
 * 包含上游、下游节点实例名，连接的插槽 ID，以及可选的激活条件。
 *
 * @param <C> 上下文类型
 * @author Refactored (注释更新)
 */
public final class EdgeDefinition<C> {
    private final String upstreamInstanceName;
    private final String outputSlotId; // 上游节点的输出槽 ID
    private final String downstreamInstanceName;
    private final String inputSlotId; // 下游节点的输入槽 ID
    private final Condition<C> condition; // 边的激活条件，默认为 alwaysTrue

    /**
     * 创建一个边的定义。
     *
     * @param upstreamInstanceName   上游节点实例名 (不能为空)
     * @param outputSlotId           上游输出槽 ID (不能为空)
     * @param downstreamInstanceName 下游节点实例名 (不能为空)
     * @param inputSlotId            下游输入槽 ID (不能为空)
     * @param condition              边的激活条件 (可以为 null，表示无条件)
     */
    public EdgeDefinition(String upstreamInstanceName, String outputSlotId,
                          String downstreamInstanceName, String inputSlotId,
                          Condition<C> condition) {
        this.upstreamInstanceName = Objects.requireNonNull(upstreamInstanceName, "上游实例名称不能为空");
        this.outputSlotId = Objects.requireNonNull(outputSlotId, "输出槽 ID 不能为空");
        this.downstreamInstanceName = Objects.requireNonNull(downstreamInstanceName, "下游实例名称不能为空");
        this.inputSlotId = Objects.requireNonNull(inputSlotId, "输入槽 ID 不能为空");
        // 如果 condition 为 null，则使用 alwaysTrue 单例
        this.condition = (condition != null) ? condition : Condition.alwaysTrue();
    }

    /**
     * 创建一个无条件边的定义。
     *
     * @param upstreamInstanceName   上游节点实例名 (不能为空)
     * @param outputSlotId           上游输出槽 ID (不能为空)
     * @param downstreamInstanceName 下游节点实例名 (不能为空)
     * @param inputSlotId            下游输入槽 ID (不能为空)
     */
    public EdgeDefinition(String upstreamInstanceName, String outputSlotId,
                          String downstreamInstanceName, String inputSlotId) {
        this(upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, null);
    }

    // --- Getters ---

    public String getUpstreamInstanceName() {
        return upstreamInstanceName;
    }

    public String getOutputSlotId() {
        return outputSlotId;
    }

    public String getDownstreamInstanceName() {
        return downstreamInstanceName;
    }

    public String getInputSlotId() {
        return inputSlotId;
    }

    public Condition<C> getCondition() {
        return condition;
    }

    // --- equals, hashCode, toString ---

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeDefinition<?> that = (EdgeDefinition<?>) o;
        // Condition 的比较依赖于其自身的 equals 实现或实例相等性
        return upstreamInstanceName.equals(that.upstreamInstanceName) &&
                outputSlotId.equals(that.outputSlotId) &&
                downstreamInstanceName.equals(that.downstreamInstanceName) &&
                inputSlotId.equals(that.inputSlotId) &&
                Objects.equals(condition, that.condition); // 使用 Objects.equals 处理 condition 可能为 alwaysTrue 单例的情况
    }

    @Override
    public int hashCode() {
        return Objects.hash(upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, condition);
    }

    @Override
    public String toString() {
        String conditionStr = (condition == Condition.alwaysTrue()) ? "AlwaysTrue" : condition.getClass().getSimpleName();
        return String.format("Edge[%s(%s) -> %s(%s), Condition: %s]",
                upstreamInstanceName, outputSlotId,
                downstreamInstanceName, inputSlotId,
                conditionStr);
    }
}
