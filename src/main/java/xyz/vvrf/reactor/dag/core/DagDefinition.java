package xyz.vvrf.reactor.dag.core;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;

/**
 * 定义一个特定上下文类型 C 的 DAG 结构 (不可变的数据结构)。
 * 由 DagDefinitionBuilder 构建。
 * 包含节点定义、边定义、计算好的执行顺序和错误处理策略。
 *
 * @param <C> 上下文类型
 * @author Refactored (注释更新)
 */
public interface DagDefinition<C> {

    /**
     * 获取 DAG 的名称。
     */
    String getDagName();

    /**
     * 获取此 DAG 定义适用的上下文类型。
     */
    Class<C> getContextType();

    /**
     * 获取计算好的拓扑执行顺序（节点实例名称列表）。
     * 列表是不可变的。
     */
    List<String> getExecutionOrder();

    /**
     * 获取所有节点定义的 Map (InstanceName -> NodeDefinition)。
     * Map 是不可变的。
     */
    Map<String, NodeDefinition> getNodeDefinitions();

    /**
     * 获取指定名称的节点定义。
     */
    Optional<NodeDefinition> getNodeDefinition(String instanceName);

    /**
     * 获取所有边定义的列表。
     * 列表是不可变的。
     */
    List<EdgeDefinition<C>> getEdgeDefinitions();

    /**
     * 获取连接到指定下游节点实例的所有入边定义。
     * 返回的列表是不可变的。
     */
    List<EdgeDefinition<C>> getIncomingEdges(String downstreamInstanceName);

    /**
     * 获取从指定上游节点实例出发的所有出边定义。
     * 返回的列表是不可变的。
     */
    List<EdgeDefinition<C>> getOutgoingEdges(String upstreamInstanceName);


    /**
     * 获取此 DAG 的错误处理策略。
     */
    ErrorHandlingStrategy getErrorHandlingStrategy();

    /**
     * 获取 DAG 中的所有节点实例名称。
     * Set 是不可变的。
     */
    Set<String> getAllNodeInstanceNames();

}
