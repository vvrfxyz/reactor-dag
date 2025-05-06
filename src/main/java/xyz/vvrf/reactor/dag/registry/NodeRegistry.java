// file: registry/NodeRegistry.java
package xyz.vvrf.reactor.dag.registry;

import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.InputSlot;
import xyz.vvrf.reactor.dag.core.OutputSlot;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * 节点注册表接口。
 * 负责管理节点类型 ID 到节点实现（或其工厂）的映射。
 * 提供获取节点元数据（输入/输出槽）的能力，用于构建时验证。
 * 新增了获取上下文类型的能力，用于引擎提供者进行管理。
 *
 * @param <C> 上下文类型
 * @author Refactored
 */
public interface NodeRegistry<C> {

    /**
     * 获取此注册表关联的上下文类型。
     *
     * @return 上下文类型的 Class 对象。
     */
    Class<C> getContextType();

    /**
     * 注册一个节点实现工厂。
     * 工厂模式允许每次需要时创建新的节点实例（如果节点有状态）。
     *
     * @param nodeTypeId 节点类型的唯一标识符
     * @param factory    创建 DagNode 实例的 Supplier
     * @throws IllegalArgumentException 如果 nodeTypeId 已被注册
     */
    void register(String nodeTypeId, Supplier<? extends DagNode<C, ?>> factory);

    /**
     * 注册一个节点实现原型（单例）。
     * 适用于无状态的节点实现。
     *
     * @param nodeTypeId 节点类型的唯一标识符
     * @param prototype  DagNode 的实例
     * @throws IllegalArgumentException 如果 nodeTypeId 已被注册
     */
    void register(String nodeTypeId, DagNode<C, ?> prototype);


    /**
     * 根据节点类型 ID 获取一个节点实例。
     * 如果注册的是工厂，则调用工厂创建新实例；如果是原型，则返回原型。
     *
     * @param nodeTypeId 节点类型 ID
     * @return DagNode 实例的 Optional，如果未注册则为空
     */
    Optional<DagNode<C, ?>> getNodeInstance(String nodeTypeId);

    /**
     * 获取指定节点类型的元信息，用于验证。
     *
     * @param nodeTypeId 节点类型 ID
     * @return 节点元信息的 Optional，如果未注册则为空
     */
    Optional<NodeMetadata> getNodeMetadata(String nodeTypeId);

    /**
     * 节点元数据，包含输入和输出槽信息。
     */
    interface NodeMetadata {
        String getTypeId();
        Set<InputSlot<?>> getInputSlots();
        OutputSlot<?> getPrimaryOutputSlot();
        Set<OutputSlot<?>> getAdditionalOutputSlots();

        default Optional<InputSlot<?>> findInputSlot(String slotId) {
            return getInputSlots().stream().filter(s -> s.getId().equals(slotId)).findFirst();
        }
        default Optional<OutputSlot<?>> findOutputSlot(String slotId) {
            if (getPrimaryOutputSlot().getId().equals(slotId)) {
                return Optional.of(getPrimaryOutputSlot());
            }
            return getAdditionalOutputSlots().stream().filter(s -> s.getId().equals(slotId)).findFirst();
        }
    }
}
