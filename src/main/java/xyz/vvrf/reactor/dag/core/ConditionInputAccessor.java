package xyz.vvrf.reactor.dag.core;

import java.util.Optional;
import java.util.Set;

/**
 * 一个特殊的 InputAccessor，供 Condition.evaluate 使用。
 * 它提供对图中 *所有* 已完成（可能是部分）节点状态和结果的只读访问。
 * 与传递给 DagNode.execute 的 InputAccessor 不同，后者只包含激活边的数据。
 *
 * @param <C> 上下文类型
 * @author Refactored (注释更新)
 */
public interface ConditionInputAccessor<C> {

    /**
     * 获取指定实例名称的节点的结果状态。
     *
     * @param instanceName 节点实例名称
     * @return 节点状态的 Optional (SUCCESS, FAILURE, SKIPPED)，如果节点尚未完成则为空。
     */
    Optional<NodeResult.NodeStatus> getNodeStatus(String instanceName);

    /**
     * 尝试获取指定实例名称的节点的 Payload。
     * 只有当该节点成功完成时才可能获取到。
     *
     * @param <P>          期望的 Payload 类型
     * @param instanceName 节点实例名称
     * @param expectedType 期望的 Payload 类型 Class 对象
     * @return 包含 Payload 的 Optional，如果节点未成功、无 Payload 或类型不匹配则为空。
     */
    <P> Optional<P> getNodePayload(String instanceName, Class<P> expectedType);

    /**
     * 获取指定实例名称的节点的错误信息（如果失败）。
     *
     * @param instanceName 节点实例名称
     * @return 包含错误的 Optional，如果节点未失败或未完成则为空。
     */
    Optional<Throwable> getNodeError(String instanceName);

    /**
     * 获取所有已完成的节点实例名称集合。
     * @return 已完成节点名称的不可变集合。
     */
    Set<String> getCompletedNodeNames();

}
