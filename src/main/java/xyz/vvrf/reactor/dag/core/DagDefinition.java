package xyz.vvrf.reactor.dag.core;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 定义一个特定上下文类型 C 的 DAG 结构。
 * 实现者负责管理节点、计算执行顺序和进行验证。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen
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
     * @param <T> 负载类型
     * @param nodeName 节点名称
     * @param payloadType 负载类型
     * @return 包含节点的 Optional，如果节点不存在则为空
     */
    <T> Optional<DagNode<C, T>> getNode(String nodeName, Class<T> payloadType);

    /**
     * 根据名称获取节点（不关心负载类型）。
     *
     * @param nodeName 节点名称
     * @return 包含节点的 Optional，如果节点不存在则为空
     */
    Optional<DagNode<C, ?>> getNodeAnyType(String nodeName);

    /**
     * 获取此 DAG 中的所有节点。
     *
     * @return 所有节点的集合
     */
    Collection<DagNode<C, ?>> getAllNodes();

    /**
     * 获取计算好的拓扑执行顺序。
     * 实现者应确保此列表在初始化后是稳定且正确的。
     *
     * @return 按拓扑顺序排列的节点名称列表
     */
    List<String> getExecutionOrder();

    /**
     * 获取指定节点支持的所有输出负载类型。
     *
     * @param nodeName 节点名称
     * @return 该节点支持的所有输出类型集合
     */
    Set<Class<?>> getSupportedOutputTypes(String nodeName);

    /**
     * 检查指定节点是否支持特定的输出负载类型。
     *
     * @param <T> 负载类型
     * @param nodeName 节点名称
     * @param payloadType 需要检查的负载类型
     * @return 如果节点支持该负载类型则返回 true
     */
    <T> boolean supportsOutputType(String nodeName, Class<T> payloadType);

    /**
     * 初始化 DAG 定义。
     * 实现者应在此方法中完成节点注册、验证（依赖、循环）和执行顺序计算。
     * 通常在构造函数或 @PostConstruct 中调用。
     *
     * @throws IllegalStateException 如果验证失败。
     */
    void initialize() throws IllegalStateException;
}