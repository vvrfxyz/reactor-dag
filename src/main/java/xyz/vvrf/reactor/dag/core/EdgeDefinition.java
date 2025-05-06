package xyz.vvrf.reactor.dag.core;

import java.util.Objects;

/**
 * DAG 中连接边的定义（纯数据）。
 * 包含上游、下游节点实例名，连接的插槽 ID，以及可选的激活条件。
 *
 * @param <C> 上下文类型
 * @author Refactored
 */
public final class EdgeDefinition<C> {
    private final String upstreamInstanceName;
    private final String outputSlotId; // 上游节点的输出槽 ID
    private final String downstreamInstanceName;
    private final String inputSlotId; // 下游节点的输入槽 ID
    private final Condition<C> condition; // 边的激活条件，默认为 alwaysTrue

    public EdgeDefinition(String upstreamInstanceName, String outputSlotId,
                          String downstreamInstanceName, String inputSlotId,
                          Condition<C> condition) {
        this.upstreamInstanceName = Objects.requireNonNull(upstreamInstanceName, "上游实例名称不能为空");
        this.outputSlotId = Objects.requireNonNull(outputSlotId, "输出槽 ID 不能为空");
        this.downstreamInstanceName = Objects.requireNonNull(downstreamInstanceName, "下游实例名称不能为空");
        this.inputSlotId = Objects.requireNonNull(inputSlotId, "输入槽 ID 不能为空");
        this.condition = (condition != null) ? condition : Condition.alwaysTrue();
    }

    // 构造函数，用于无条件边
    public EdgeDefinition(String upstreamInstanceName, String outputSlotId,
                          String downstreamInstanceName, String inputSlotId) {
        this(upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, null);
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeDefinition<?> that = (EdgeDefinition<?>) o;
        // Condition 的比较比较棘手，如果不是同一个实例，equals 可能不准确
        // 这里假设 Condition 实现具有有意义的 equals 或依赖于实例相等
        return upstreamInstanceName.equals(that.upstreamInstanceName) &&
                outputSlotId.equals(that.outputSlotId) &&
                downstreamInstanceName.equals(that.downstreamInstanceName) &&
                inputSlotId.equals(that.inputSlotId) &&
                Objects.equals(condition, that.condition); // 使用 Objects.equals 处理 null
    }

    @Override
    public int hashCode() {
        return Objects.hash(upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, condition);
    }

    @Override
    public String toString() {
        return "EdgeDefinition{" +
                "upstream='" + upstreamInstanceName + '\'' +
                ", outputSlot='" + outputSlotId + '\'' +
                ", downstream='" + downstreamInstanceName + '\'' +
                ", inputSlot='" + inputSlotId + '\'' +
                ", hasCondition=" + (condition != Condition.alwaysTrue()) + // 简化显示
                '}';
    }
}