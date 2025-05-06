package xyz.vvrf.reactor.dag.core;

import java.util.Optional;
import java.util.Set;

/**
 * 一个特殊的 InputAccessor，主要供 {@link DeclaredDependencyCondition} 使用。
 * 它提供对图中 *允许访问* 的已完成节点状态和结果的只读访问。
 * 访问范围由引擎根据条件声明的依赖来限制。
 *
 * @param <C> 上下文类型
 * @author 重构者 (注释更新)
 */
public interface ConditionInputAccessor<C> { // 不再是 @FunctionalInterface

    /**
     * 获取指定实例名称的节点的结果状态。
     * 只有当该节点被允许访问且已完成时，才能获取。
     *
     * @param instanceName 节点实例名称
     * @return 节点状态的 Optional (SUCCESS, FAILURE, SKIPPED)，如果节点不允许访问或未完成则为空。
     * @throws IllegalArgumentException 如果尝试访问未声明依赖的节点（取决于实现）。
     */
    Optional<NodeResult.NodeStatus> getNodeStatus(String instanceName);

    /**
     * 尝试获取指定实例名称的节点的 Payload。
     * 只有当该节点被允许访问、成功完成且有 Payload 时才可能获取到。
     *
     * @param <P>          期望的 Payload 类型
     * @param instanceName 节点实例名称
     * @param expectedType 期望的 Payload 类型 Class 对象
     * @return 包含 Payload 的 Optional，如果节点不允许访问、未成功、无 Payload 或类型不匹配则为空。
     * @throws IllegalArgumentException 如果尝试访问未声明依赖的节点（取决于实现）。
     */
    <P> Optional<P> getNodePayload(String instanceName, Class<P> expectedType);

    /**
     * 获取指定实例名称的节点的错误信息（如果失败）。
     * 只有当该节点被允许访问且执行失败时才能获取。
     *
     * @param instanceName 节点实例名称
     * @return 包含错误的 Optional，如果节点不允许访问、未失败或未完成则为空。
     * @throws IllegalArgumentException 如果尝试访问未声明依赖的节点（取决于实现）。
     */
    Optional<Throwable> getNodeError(String instanceName);

    /**
     * 获取所有被允许访问且已完成的节点实例名称集合。
     * @return 允许访问且已完成节点名称的不可变集合。
     */
    Set<String> getCompletedNodeNames(); // 语义可能需要调整，是返回允许的还是允许且完成的？

}
