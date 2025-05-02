// [file name]: DagDefinition.java
package xyz.vvrf.reactor.dag.core;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * 定义一个特定上下文类型 C 的 DAG 结构。
 * 实现者负责管理节点定义 (DagNodeDefinition)、计算执行顺序和进行验证。
 * 依赖关系在 DagNodeDefinition 中定义。
 *
 * @param <C> 上下文类型
 */
public interface DagDefinition<C> {

    /**
     * 获取 DAG 的名称或标识符。
     *
     * @return DAG 名称
     */
    String getDagName();

    /**
     * 获取此 DAG 定义适用的上下文类型。
     *
     * @return 上下文类型
     */
    Class<C> getContextType();

    /**
     * 根据名称获取节点定义。
     *
     * @param nodeName 节点名称
     * @return 包含节点定义的 Optional，如果节点不存在则为空
     */
    Optional<DagNodeDefinition<C, ?>> getNodeDefinition(String nodeName);

    /**
     * 获取此 DAG 中的所有节点定义。
     *
     * @return 所有节点定义的集合
     */
    Collection<DagNodeDefinition<C, ?>> getAllNodeDefinitions();

    /**
     * 获取计算好的拓扑执行顺序。
     * 实现者应确保此列表在初始化后是稳定且正确的。
     *
     * @return 按拓扑顺序排列的节点名称列表
     */
    List<String> getExecutionOrder();

    /**
     * 初始化 DAG 定义。
     * 实现者应在此方法中完成节点注册验证、依赖关系验证（仅名称）、循环检测和执行顺序计算。
     * 必须在所有节点通过 Builder 添加完成后调用。
     *
     * @throws IllegalStateException 如果验证失败。
     */
    void initialize() throws IllegalStateException;

    /**
     * 检查 DAG 定义是否已初始化。
     * @return 如果已初始化则返回 true
     */
    boolean isInitialized();
}
