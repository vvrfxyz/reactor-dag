// file: core/DagDefinition.java
package xyz.vvrf.reactor.dag.core;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 定义一个特定上下文类型 C 的 DAG 结构 (不可变)。
 * 由 DagDefinitionBuilder 构建。
 * 包含节点实例、连接关系、执行顺序和错误处理策略。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen (Refactored)
 */
public interface DagDefinition<C> {

    /**
     * 获取 DAG 的名称。
     * @return DAG 名称
     */
    String getDagName();

    /**
     * 获取此 DAG 定义适用的上下文类型。
     * @return 上下文类型
     */
    Class<C> getContextType();

    /**
     * 获取计算好的拓扑执行顺序。
     * @return 按拓扑顺序排列的节点实例名称列表
     */
    List<String> getExecutionOrder();

    /**
     * 获取指定名称的节点实例的实现。
     * @param instanceName 节点实例的名称 (在构建时指定)
     * @return 包含节点实现的 Optional，如果实例不存在则为空
     */
    Optional<DagNode<C, ?>> getNodeImplementation(String instanceName);

    /**
     * 获取指定节点实例的输出 Payload 类型。
     * @param instanceName 节点实例的名称
     * @return 输出类型的 Optional
     */
    Optional<Class<?>> getNodeOutputType(String instanceName);

    /**
     * 获取指定节点实例的输入连接信息。
     * @param instanceName 节点实例的名称
     * @return Map<String, String> Key: 输入槽逻辑名称, Value: 提供输入的上游节点实例名称。如果节点不存在或无输入，则为空 Map。
     */
    Map<String, String> getUpstreamWiring(String instanceName);

    /**
     * 获取此 DAG 的错误处理策略。
     * @return 错误处理策略
     */
    ErrorHandlingStrategy getErrorHandlingStrategy();

    /**
     * 获取 DAG 中的所有节点实例名称。
     * @return 所有节点实例名称的集合
     */
    java.util.Set<String> getAllNodeInstanceNames();
}
