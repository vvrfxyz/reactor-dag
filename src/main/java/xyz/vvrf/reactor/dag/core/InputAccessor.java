package xyz.vvrf.reactor.dag.core;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * 提供对节点所需输入数据的安全、类型化的访问。
 * 这是传递给 {@link DagNode#execute(Object, InputAccessor)} 方法的参数类型。
 * 它只提供通过激活的（条件满足的）、来自成功上游节点的边传入的数据。
 *
 * @param <C> 上下文类型 (虽然接口方法不直接使用 C，但与 DagNode 保持一致)
 * @author Ruifeng.wen
 */
public interface InputAccessor<C> {

    /**
     * 安全地获取指定输入槽的 Payload。
     * 只有当连接到此槽的上游节点成功执行，并且连接边被激活（条件满足）时，才会返回 Payload。
     * 如果有多个激活的边连接到同一个输入槽，此方法的行为取决于实现。
     * 对于 {@link xyz.vvrf.reactor.dag.execution.DefaultInputAccessor}，它会返回遇到的第一个可用的、类型匹配的 Payload。
     *
     * @param <T>       期望的 Payload 类型 (由 InputSlot 定义)
     * @param inputSlot 节点声明的类型化输入槽
     * @return 包含符合类型 Payload 的 Optional，否则为空
     */
    <T> Optional<T> getPayload(InputSlot<T> inputSlot);

    /**
     * 获取指定输入槽的 Payload，如果不可用则抛出 {@link NoSuchElementException}。
     *
     * @param <T>       期望的 Payload 类型
     * @param inputSlot 输入槽
     * @return Payload (不能为空)
     * @throws NoSuchElementException 如果找不到匹配的、可用的 Payload
     */
    default <T> T getPayloadOrThrow(InputSlot<T> inputSlot) {
        return getPayload(inputSlot)
                .orElseThrow(() -> new NoSuchElementException(
                        String.format("输入槽 '%s' (类型: %s) 没有可用的负载数据",
                                inputSlot.getId(), inputSlot.getType().getSimpleName())));
    }

    /**
     * 获取指定输入槽的 Payload，如果不可用则返回指定的默认值。
     *
     * @param <T>          期望的 Payload 类型
     * @param inputSlot    输入槽
     * @param defaultValue 如果找不到 Payload 时返回的默认值
     * @return Payload 或默认值
     */
    default <T> T getPayloadOrDefault(InputSlot<T> inputSlot, T defaultValue) {
        return getPayload(inputSlot).orElse(defaultValue);
    }

    /**
     * 检查指定的输入槽是否有可用的、来自成功上游的 Payload 通过激活的边传入。
     * 如果有多个上游连接到此槽，只要其中至少一个提供了可用的、类型匹配的 Payload，此方法即返回 true。
     *
     * @param inputSlot 输入槽
     * @return 如果输入槽有可用数据，则返回 true
     */
    boolean isAvailable(InputSlot<?> inputSlot);

    /**
     * 检查连接到指定输入槽的上游节点是否执行失败。
     * 注意：即使上游失败，如果错误处理策略是 CONTINUE_ON_FAILURE，本节点仍可能执行（如果其他输入满足）。
     * 如果有多个边连接到此槽，只要有一个上游失败，此方法就可能返回 true（取决于实现细节）。
     *
     * @param inputSlot 输入槽
     * @return 如果连接到此槽的某个上游节点执行失败，则返回 true
     */
    boolean isFailed(InputSlot<?> inputSlot);

    /**
     * 检查连接到指定输入槽的上游节点是否被跳过。
     * 如果有多个边连接到此槽，只要有一个上游被跳过，此方法就可能返回 true。
     *
     * @param inputSlot 输入槽
     * @return 如果连接到此槽的某个上游节点被跳过，则返回 true
     */
    boolean isSkipped(InputSlot<?> inputSlot);

    /**
     * 检查连接到指定输入槽的边是否未定义或未激活（例如，条件评估为 false 或上游未成功）。
     * 如果有多个边连接，此方法通常表示没有任何一个边是激活且上游成功的。
     *
     * @param inputSlot 输入槽
     * @return 如果没有激活的、成功的边连接到此槽，则返回 true
     */
    boolean isInactive(InputSlot<?> inputSlot);


    /**
     * 获取连接到指定输入槽的上游节点的错误信息（如果失败）。
     * 如果有多个失败的上游连接到此槽，可能只返回其中一个错误。
     *
     * @param inputSlot 输入槽
     * @return 包含错误的 Optional，如果上游未失败或边未连接/未激活，则为空
     */
    Optional<Throwable> getError(InputSlot<?> inputSlot);

    /**
     * 检索来自所有连接到指定输入槽的、激活且成功的上游节点的全部 Payload。
     * 列表中的每个 {@code Optional<T>} 对应一个这样的上游。如果某个上游节点成功执行但未产生 Payload
     * (即其 Payload 为 null)，则该上游对应的 {@code Optional<T>} 将为空。
     * Payload 在列表中的顺序通常对应于激活边被处理或定义的顺序，但具体实现可能会提供不同的顺序保证。
     *
     * @param <T>       期望的 Payload 类型。
     * @param inputSlot 要检索 Payload 的输入槽。
     * @return 一个 {@code List<Optional<T>>}，其中每个元素包含一个类型为 T 的 Payload (如果可用且类型兼容)。
     * 如果没有任何激活且成功的上游为此槽提供数据，则返回空列表。
     */
    <T> List<Optional<T>> getAllPayloads(InputSlot<T> inputSlot);
}
