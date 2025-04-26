package xyz.vvrf.reactor.dag.core;

import java.util.Objects;

/**
 * 描述 DAG 中节点之间的执行顺序依赖关系 (边)。
 * 这定义了哪个节点必须在另一个节点之前执行，但不直接关联数据流。
 * 数据流通过 InputRequirement 和 InputMapping 定义。
 *
 * @author ruifeng.wen
 */
public final class ExecutionEdge {
    private final String sourceNodeName;
    private final String targetNodeName;

    /**
     * 创建执行边描述符
     *
     * @param sourceNodeName 必须先执行的节点名称
     * @param targetNodeName 依赖于源节点执行完成的节点名称
     * @throws NullPointerException 如果任一名称为 null
     */
    public ExecutionEdge(String sourceNodeName, String targetNodeName) {
        this.sourceNodeName = Objects.requireNonNull(sourceNodeName, "源节点名称不能为空");
        this.targetNodeName = Objects.requireNonNull(targetNodeName, "目标节点名称不能为空");
    }

    /**
     * 获取源节点名称 (边的起点)
     * @return 源节点名称
     */
    public String getSourceNodeName() {
        return sourceNodeName;
    }

    /**
     * 获取目标节点名称 (边的终点)
     * @return 目标节点名称
     */
    public String getTargetNodeName() {
        return targetNodeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExecutionEdge that = (ExecutionEdge) o;
        return sourceNodeName.equals(that.sourceNodeName) && targetNodeName.equals(that.targetNodeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNodeName, targetNodeName);
    }

    @Override
    public String toString() {
        return String.format("ExecutionEdge[%s -> %s]", sourceNodeName, targetNodeName);
    }
}
