package xyz.vvrf.reactor.dag.registry;

import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.InputSlot;
import xyz.vvrf.reactor.dag.core.OutputSlot;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * 节点注册表接口。
 * 负责管理节点类型 ID 到节点实现（或其工厂/原型）的映射。
 * 提供获取节点元数据（输入/输出槽）的能力，用于图构建时验证。
 * 每个注册表实例关联一个特定的上下文类型 C。
 *
 * @param <C> 上下文类型
 * @author Refactored (注释更新)
 */
public interface NodeRegistry<C> {

    /**
     * 获取此注册表关联的上下文类型。
     * 这是区分不同注册表的关键。
     *
     * @return 上下文类型的 Class 对象 (不能为空)。
     */
    Class<C> getContextType();

    /**
     * 注册一个节点实现工厂。
     * 工厂模式允许每次需要时创建新的节点实例（适用于有状态的节点）。
     *
     * @param nodeTypeId 节点类型的唯一标识符 (在此注册表内唯一, 不能为空)
     * @param factory    创建 DagNode<C, ?> 实例的 Supplier (不能为空, 不能返回 null)
     * @throws IllegalArgumentException 如果 nodeTypeId 已被注册，或 factory 无效。
     */
    void register(String nodeTypeId, Supplier<? extends DagNode<C, ?>> factory);

    /**
     * 注册一个节点实现原型（通常是单例）。
     * 适用于无状态、线程安全的节点实现。注册表将存储此实例并每次返回它。
     *
     * @param nodeTypeId 节点类型的唯一标识符 (在此注册表内唯一, 不能为空)
     * @param prototype  DagNode<C, ?> 的实例 (不能为空)
     * @throws IllegalArgumentException 如果 nodeTypeId 已被注册。
     */
    void register(String nodeTypeId, DagNode<C, ?> prototype);


    /**
     * 根据节点类型 ID 获取一个节点实例。
     * 如果注册的是工厂，则调用工厂创建新实例；如果是原型，则返回原型实例。
     *
     * @param nodeTypeId 节点类型 ID (不能为空)
     * @return DagNode 实例的 Optional，如果该类型 ID 未在此注册表中注册，则为空。
     */
    Optional<DagNode<C, ?>> getNodeInstance(String nodeTypeId);

    /**
     * 获取指定节点类型的元信息（输入/输出槽定义），主要用于图构建时的验证。
     *
     * @param nodeTypeId 节点类型 ID (不能为空)
     * @return 节点元信息的 Optional，如果该类型 ID 未在此注册表中注册，则为空。
     */
    Optional<NodeMetadata> getNodeMetadata(String nodeTypeId);

    /**
     * 节点元数据接口，包含节点的静态信息（插槽定义）。
     */
    interface NodeMetadata {
        /** 获取节点类型 ID */
        String getTypeId();
        /** 获取所有输入槽定义的不可变集合 */
        Set<InputSlot<?>> getInputSlots();
        /** 获取主输出槽 */
        OutputSlot<?> getPrimaryOutputSlot();
        /** 获取其他命名输出槽定义的不可变集合 */
        Set<OutputSlot<?>> getAdditionalOutputSlots();

        /** 辅助方法：根据 ID 查找输入槽 */
        default Optional<InputSlot<?>> findInputSlot(String slotId) {
            return getInputSlots().stream().filter(s -> s.getId().equals(slotId)).findFirst();
        }
        /** 辅助方法：根据 ID 查找输出槽 (包括主输出槽) */
        default Optional<OutputSlot<?>> findOutputSlot(String slotId) {
            if (getPrimaryOutputSlot().getId().equals(slotId)) {
                return Optional.of(getPrimaryOutputSlot());
            }
            return getAdditionalOutputSlots().stream().filter(s -> s.getId().equals(slotId)).findFirst();
        }
    }
}
