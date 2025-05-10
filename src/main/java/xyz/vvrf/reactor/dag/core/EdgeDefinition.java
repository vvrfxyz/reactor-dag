package xyz.vvrf.reactor.dag.core;

import java.util.Objects;
import java.util.Set;

/**
 * DAG 中连接边的定义（不可变数据类）。
 * 包含上游、下游节点实例名，连接的插槽 ID，以及激活条件（ConditionBase 类型）。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen
 */
public final class EdgeDefinition<C> {
    private final String upstreamInstanceName;
    private final String outputSlotId;
    private final String downstreamInstanceName;
    private final String inputSlotId;
    private final ConditionBase<C> condition;

    /**
     * 创建一个边的定义。
     *
     * @param upstreamInstanceName   上游节点实例名 (不能为空)
     * @param outputSlotId           上游输出槽 ID (不能为空)
     * @param downstreamInstanceName 下游节点实例名 (不能为空)
     * @param inputSlotId            下游输入槽 ID (不能为空)
     * @param condition              边的激活条件 (ConditionBase 类型)。如果为 null，则使用 {@link DirectUpstreamCondition#alwaysTrue()}。
     */
    public EdgeDefinition(String upstreamInstanceName, String outputSlotId,
                          String downstreamInstanceName, String inputSlotId,
                          ConditionBase<C> condition) {
        this.upstreamInstanceName = Objects.requireNonNull(upstreamInstanceName, "上游实例名称不能为空");
        this.outputSlotId = Objects.requireNonNull(outputSlotId, "输出槽 ID 不能为空");
        this.downstreamInstanceName = Objects.requireNonNull(downstreamInstanceName, "下游实例名称不能为空");
        this.inputSlotId = Objects.requireNonNull(inputSlotId, "输入槽 ID 不能为空");
        // 如果 condition 为 null，则使用 alwaysTrue 单例
        this.condition = (condition == null)
                ? DirectUpstreamCondition.alwaysTrue()
                : condition;
    }

    /**
     * 创建一个无条件边的定义 (使用 {@link DirectUpstreamCondition#alwaysTrue()})。
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

    /**
     * 获取边的条件对象。
     * @return ConditionBase<C> 实例。永远不会是 null。
     */
    public ConditionBase<C> getCondition() {
        return condition;
    }

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
                Objects.equals(condition, that.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, condition);
    }

    @Override
    public String toString() {
        String conditionStr;
        if (condition instanceof DirectUpstreamCondition.AlwaysTrueCondition) {
            conditionStr = "AlwaysTrue";
        } else if (condition instanceof DirectUpstreamCondition) {
            conditionStr = condition.getClass().getSimpleName() + " (Direct)";
        } else if (condition instanceof LocalInputCondition) {
            conditionStr = condition.getClass().getSimpleName() + " (Local)";
        } else if (condition instanceof DeclaredDependencyCondition) {
            Set<String> deps = ((DeclaredDependencyCondition<C>) condition).getRequiredNodeDependencies();
            conditionStr = condition.getClass().getSimpleName() + " (Deps: " + (deps == null || deps.isEmpty() ? "None" : deps) + ")";
        } else {
            conditionStr = condition.getClass().getName() + " (Unknown Type)";
        }

        return String.format("Edge[%s(%s) -> %s(%s), Condition: %s]",
                upstreamInstanceName, outputSlotId,
                downstreamInstanceName, inputSlotId,
                conditionStr);
    }
}
