package xyz.vvrf.reactor.dag.core;

import java.util.Optional;
import java.util.Set;

/**
 * 为 {@link LocalInputCondition} 提供对其目标下游节点的所有直接上游节点结果的访问能力。
 *
 * @param <C> 上下文类型
 * @author 重构者
 */
public interface LocalInputAccessor<C> {

    /**
     * 获取指定名称的直接上游节点的结果。
     *
     * @param upstreamInstanceName 直接上游节点的实例名称
     * @return 包含该上游节点结果的 Optional，如果该名称不是直接上游或尚未完成，则为空。
     */
    Optional<NodeResult<C, ?>> getUpstreamResult(String upstreamInstanceName);

    /**
     * 获取所有直接上游节点的实例名称集合。
     *
     * @return 直接上游节点名称的不可变集合。
     */
    Set<String> getDirectUpstreamNames();

    /**
     * 检查指定的直接上游节点是否成功。
     *
     * @param upstreamInstanceName 直接上游节点的实例名称
     * @return 如果该上游节点存在且成功，则返回 true。
     */
    default boolean isUpstreamSuccess(String upstreamInstanceName) {
        return getUpstreamResult(upstreamInstanceName)
                .map(NodeResult::isSuccess)
                .orElse(false);
    }

    /**
     * 尝试获取指定直接上游节点的 Payload。
     *
     * @param <P>                期望的 Payload 类型
     * @param upstreamInstanceName 直接上游节点的实例名称
     * @param expectedType       期望的 Payload 类型 Class 对象
     * @return 包含 Payload 的 Optional，如果上游未成功、无 Payload 或类型不匹配则为空。
     */
    default <P> Optional<P> getUpstreamPayload(String upstreamInstanceName, Class<P> expectedType) {
        return getUpstreamResult(upstreamInstanceName)
                .filter(NodeResult::isSuccess)
                .flatMap(NodeResult::getPayload)
                .filter(expectedType::isInstance)
                .map(expectedType::cast);
    }

    // 可以根据需要添加更多便捷方法，例如 getUpstreamError, isUpstreamFailed 等
}
