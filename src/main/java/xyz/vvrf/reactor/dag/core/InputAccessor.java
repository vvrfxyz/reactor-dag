// file: core/InputAccessor.java
package xyz.vvrf.reactor.dag.core;

import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * 提供对节点所需输入数据的安全、便捷访问。
 * 这是传递给 DagNode.execute() 方法的参数类型。
 * 它根据构建时定义的连接关系获取上游节点的 Payload。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen (Refactored)
 */
public interface InputAccessor<C> {

    /**
     * 安全地获取指定输入槽的 Payload。
     * 内部会检查连接是否存在、上游节点是否成功执行、Payload 是否存在以及类型是否匹配。
     *
     * @param <InputP>     期望的 Payload 类型
     * @param inputSlotName 节点声明的输入槽逻辑名称 (来自 getInputRequirements)
     * @param expectedType 期望的 Payload 类型的 Class 对象
     * @return 包含符合类型 Payload 的 Optional，否则为空
     */
    <InputP> Optional<InputP> getPayload(String inputSlotName, Class<InputP> expectedType);

    /**
     * 获取指定输入槽的 Payload，如果不存在或类型不匹配则抛出异常。
     *
     * @param <InputP>     期望的 Payload 类型
     * @param inputSlotName 输入槽逻辑名称
     * @param expectedType 期望的 Payload 类型
     * @return Payload
     * @throws NoSuchElementException 如果找不到匹配的 Payload
     */
    default <InputP> InputP getPayloadOrThrow(String inputSlotName, Class<InputP> expectedType) {
        return getPayload(inputSlotName, expectedType)
                .orElseThrow(() -> new NoSuchElementException(
                        String.format("No payload of type %s found for input slot '%s'",
                                expectedType.getSimpleName(), inputSlotName)));
    }

    /**
     * 获取指定输入槽的 Payload，如果不存在或类型不匹配则返回默认值。
     *
     * @param <InputP>     期望的 Payload 类型
     * @param inputSlotName 输入槽逻辑名称
     * @param expectedType 期望的 Payload 类型
     * @param defaultValue 默认值
     * @return Payload 或默认值
     */
    default <InputP> InputP getPayloadOrDefault(String inputSlotName, Class<InputP> expectedType, InputP defaultValue) {
        return getPayload(inputSlotName, expectedType).orElse(defaultValue);
    }

    /**
     * 检查指定的输入槽是否有可用的、成功的上游 Payload。
     *
     * @param inputSlotName 输入槽逻辑名称
     * @return 如果输入槽已连接，上游节点成功且有 Payload，则返回 true
     */
    boolean isInputAvailable(String inputSlotName);

    /**
     * 检查连接到指定输入槽的上游节点是否执行失败。
     *
     * @param inputSlotName 输入槽逻辑名称
     * @return 如果输入槽已连接且上游节点执行失败，则返回 true
     */
    boolean isInputFailed(String inputSlotName);

    /**
     * 检查连接到指定输入槽的上游节点是否被跳过。
     *
     * @param inputSlotName 输入槽逻辑名称
     * @return 如果输入槽已连接且上游节点被跳过，则返回 true
     */
    boolean isInputSkipped(String inputSlotName);

    /**
     * 获取连接到指定输入槽的上游节点的错误信息（如果失败）。
     *
     * @param inputSlotName 输入槽逻辑名称
     * @return 包含错误的 Optional，如果输入未连接、上游未失败或未执行，则为空
     */
    Optional<Throwable> getInputError(String inputSlotName);
}
