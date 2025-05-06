package xyz.vvrf.reactor.dag.core;

import xyz.vvrf.reactor.dag.core.InputSlot;

import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * 提供对节点所需输入数据的安全、类型化的访问。
 * 这是传递给 DagNode.execute() 方法的参数类型。
 * 它只提供通过激活的、条件满足的边传入的数据。
 *
 * @param <C> 上下文类型
 * @author Refactored
 */
public interface InputAccessor<C> {

    /**
     * 安全地获取指定输入槽的 Payload。
     * 只有当连接到此槽的上游节点成功执行，并且连接边被激活（条件满足）时，才会返回 Payload。
     *
     * @param <T>       期望的 Payload 类型 (由 InputSlot 定义)
     * @param inputSlot 节点声明的类型化输入槽
     * @return 包含符合类型 Payload 的 Optional，否则为空
     */
    <T> Optional<T> getPayload(InputSlot<T> inputSlot);

    /**
     * 获取指定输入槽的 Payload，如果不可用则抛出异常。
     *
     * @param <T>       期望的 Payload 类型
     * @param inputSlot 输入槽
     * @return Payload
     * @throws NoSuchElementException 如果找不到匹配的、可用的 Payload
     */
    default <T> T getPayloadOrThrow(InputSlot<T> inputSlot) {
        return getPayload(inputSlot)
                .orElseThrow(() -> new NoSuchElementException(
                        String.format("No available payload found for input slot '%s' (Type: %s)",
                                inputSlot.getId(), inputSlot.getType().getSimpleName())));
    }

    /**
     * 获取指定输入槽的 Payload，如果不可用则返回默认值。
     *
     * @param <T>          期望的 Payload 类型
     * @param inputSlot    输入槽
     * @param defaultValue 默认值
     * @return Payload 或默认值
     */
    default <T> T getPayloadOrDefault(InputSlot<T> inputSlot, T defaultValue) {
        return getPayload(inputSlot).orElse(defaultValue);
    }

    /**
     * 检查指定的输入槽是否有可用的、成功的上游 Payload 通过激活的边传入。
     *
     * @param inputSlot 输入槽
     * @return 如果输入槽有可用数据，则返回 true
     */
    boolean isAvailable(InputSlot<?> inputSlot);

    /**
     * 检查连接到指定输入槽的上游节点是否执行失败。
     * 注意：即使上游失败，如果错误处理策略是 CONTINUE_ON_FAILURE，本节点仍可能执行（如果其他输入满足）。
     *
     * @param inputSlot 输入槽
     * @return 如果连接到此槽的上游节点执行失败，则返回 true
     */
    boolean isFailed(InputSlot<?> inputSlot);

    /**
     * 检查连接到指定输入槽的上游节点是否被跳过。
     *
     * @param inputSlot 输入槽
     * @return 如果连接到此槽的上游节点被跳过，则返回 true
     */
    boolean isSkipped(InputSlot<?> inputSlot);

    /**
     * 检查连接到指定输入槽的边是否未定义或未激活（例如，条件评估为 false）。
     *
     * @param inputSlot 输入槽
     * @return 如果边未连接或未激活，则返回 true
     */
    boolean isInactive(InputSlot<?> inputSlot);


    /**
     * 获取连接到指定输入槽的上游节点的错误信息（如果失败）。
     *
     * @param inputSlot 输入槽
     * @return 包含错误的 Optional，如果上游未失败或边未连接/未激活，则为空
     */
    Optional<Throwable> getError(InputSlot<?> inputSlot);
}