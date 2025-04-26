package xyz.vvrf.reactor.dag.core;

import java.util.Collection;
import java.util.List;
import java.util.Map; // 引入 Map
import java.util.Optional;
import java.util.Set;

/**
 * 定义一个特定上下文类型 C 的 DAG 结构。
 * 实现者负责管理节点、计算执行顺序、验证输入映射和执行依赖。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen (Refactored by Devin)
 */
public interface DagDefinition<C> {

    /**
     * 获取 DAG 的名称或标识符 (可选, 用于日志或管理)
     *
     * @return DAG 名称
     */
    default String getDagName() {
        return this.getClass().getSimpleName();
    }

    /**
     * 获取此 DAG 定义适用的上下文类型。
     * 主要用于类型安全和查找。
     *
     * @return 上下文类型
     */
    Class<C> getContextType();

    /**
     * 根据名称和期望的负载类型获取节点。
     *
     * @param <P>         期望的节点 Payload 类型
     * @param nodeName    节点名称
     * @param payloadType 期望的 Payload 类型 (节点输出类型必须兼容此类型)
     * @return 包含节点的 Optional，如果节点不存在或类型不匹配则为空。返回的节点事件类型为通配符。
     */
    <P> Optional<DagNode<C, P, ?>> getNode(String nodeName, Class<P> payloadType);

    /**
     * 根据名称获取节点（不关心负载和事件类型）。
     *
     * @param nodeName 节点名称
     * @return 包含节点的 Optional，如果节点不存在则为空
     */
    Optional<DagNode<C, ?, ?>> getNodeAnyType(String nodeName);

    /**
     * 获取此 DAG 中的所有节点。
     *
     * @return 所有节点的集合 (Payload 和 Event 类型为通配符)
     */
    Collection<DagNode<C, ?, ?>> getAllNodes();

    /**
     * 获取计算好的拓扑执行顺序。
     * 实现者应确保此列表在初始化后是稳定且正确的。
     *
     * @return 按拓扑顺序排列的节点名称列表
     */
    List<String> getExecutionOrder();

    /**
     * 获取指定节点支持的所有输出 Payload 类型。
     *
     * @param nodeName 节点名称
     * @return 该节点支持的所有 Payload 类型集合 (通常只有一个)
     */
    Set<Class<?>> getSupportedOutputTypes(String nodeName);

    /**
     * 检查指定节点是否支持特定的输出 Payload 类型。
     *
     * @param <P>         Payload 类型
     * @param nodeName    节点名称
     * @param payloadType 需要检查的 Payload 类型
     * @return 如果节点输出类型兼容 (isAssignableFrom) 该 Payload 类型则返回 true
     */
    <P> boolean supportsOutputType(String nodeName, Class<P> payloadType);

    /**
     * 初始化 DAG 定义。
     * 实现者应在此方法中完成节点注册、验证（执行依赖、循环、输入映射）和执行顺序计算。
     * 必须在所有执行依赖和输入映射通过 Builder 设置完成后调用。
     *
     * @throws IllegalStateException 如果验证失败。
     */
    void initialize() throws IllegalStateException;

    /**
     * 检查 DAG 定义是否已初始化。
     * @return 如果已初始化则返回 true
     */
    boolean isInitialized();

    /**
     * 获取指定节点的输入映射配置。
     * @param nodeName 节点名称
     * @return 该节点的输入映射 (InputRequirement -> sourceNodeName)，如果无映射则为空 Map。
     */
    Map<InputRequirement<?>, String> getInputMappingForNode(String nodeName);

    /**
     * 获取指定节点的直接执行前驱节点名称集合。
     * @param nodeName 节点名称
     * @return 前驱节点名称集合，如果无前驱则为空 Set。
     */
    Set<String> getExecutionPredecessors(String nodeName);
}
