package xyz.vvrf.reactor.dag.core;

import reactor.core.publisher.Flux;

import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * 提供对节点依赖执行结果的安全、便捷访问。
 * 这是传递给 DagNode.execute() 方法的参数类型。
 * 访问依赖项基于类型和可选的限定符，而不是依赖节点的名称。
 *
 * @param <C> 上下文类型
 * @author Devin (AI Software Engineer)
 */
public interface InputDependencyAccessor<C> {

    /**
     * 根据类型获取必需的 Payload。
     * 如果 DAG 配置中没有为此类型提供唯一的、成功的上游源，则可能抛出异常或返回错误状态的 Optional。
     *
     * @param <DepP>       期望的 Payload 类型
     * @param expectedType 期望的 Payload 类型的 Class 对象
     * @return 包含符合类型 Payload 的 Optional，如果找不到或上游失败则为空。
     * @throws NoSuchElementException 如果必需的输入未找到或来源节点失败/跳过。
     * @throws IllegalStateException 如果存在多个未限定的来源提供相同类型。
     */
    <DepP> DepP getRequiredPayload(Class<DepP> expectedType);

    /**
     * 根据类型和限定符获取必需的 Payload。
     *
     * @param <DepP>       期望的 Payload 类型
     * @param expectedType 期望的 Payload 类型的 Class 对象
     * @param qualifier    用于区分相同类型的输入的限定符
     * @return 包含符合类型 Payload 的 Optional，如果找不到或上游失败则为空。
     * @throws NoSuchElementException 如果必需的输入未找到或来源节点失败/跳过。
     */
    <DepP> DepP getRequiredPayload(Class<DepP> expectedType, String qualifier);

    /**
     * 根据类型获取可选的 Payload。
     * 如果找不到、来源节点失败/跳过或未配置，则返回 Optional.empty()。
     *
     * @param <DepP>       期望的 Payload 类型
     * @param expectedType 期望的 Payload 类型的 Class 对象
     * @return 包含符合类型 Payload 的 Optional，否则为空。
     * @throws IllegalStateException 如果存在多个未限定的来源提供相同类型。
     */
    <DepP> Optional<DepP> getOptionalPayload(Class<DepP> expectedType);

    /**
     * 根据类型和限定符获取可选的 Payload。
     * 如果找不到、来源节点失败/跳过或未配置，则返回 Optional.empty()。
     *
     * @param <DepP>       期望的 Payload 类型
     * @param expectedType 期望的 Payload 类型的 Class 对象
     * @param qualifier    用于区分相同类型的输入的限定符
     * @return 包含符合类型 Payload 的 Optional，否则为空。
     */
    <DepP> Optional<DepP> getOptionalPayload(Class<DepP> expectedType, String qualifier);

    /**
     * 根据输入需求对象获取 Payload。
     * 这是获取 Payload 的统一入口。
     *
     * @param <DepP>      期望的 Payload 类型
     * @param requirement 输入需求描述对象
     * @return 包含 Payload 的 Optional。对于必需输入，如果未找到或源失败，会抛出异常。对于可选输入，则返回 empty。
     * @throws NoSuchElementException 如果必需的输入未找到或来源节点失败/跳过。
     * @throws IllegalStateException 如果存在多个未限定的来源提供相同类型且未指定限定符。
     */
    <DepP> Optional<DepP> getPayload(InputRequirement<DepP> requirement);


    /**
     * 获取所有直接上游（执行顺序上的前驱）节点产生的事件流的合并。
     * 注意：返回的 Flux 可能在节点执行时仍然是活动的（流式处理）。
     * 失败或被跳过的上游节点的事件流通常会被忽略。
     *
     * @return 合并后的事件流 Flux<Event<?>> (可能为空)
     */
    Flux<Event<?>> getAllUpstreamEvents();

    /**
     * 获取指定源节点的完整 NodeResult。
     * 注意：这暴露了实现细节（源节点名称），应谨慎使用。
     * 主要用于 shouldExecute 或需要访问原始状态/错误的场景。
     *
     * @param sourceNodeName 源节点的名称 (在执行顺序上是当前节点的前驱)
     * @return 包含 NodeResult 的 Optional，如果该源节点不存在或未执行则为空
     */
    Optional<NodeResult<C, ?, ?>> getSourceResult(String sourceNodeName);

    /**
     * 检查指定的源节点是否成功执行。
     *
     * @param sourceNodeName 源节点名称
     * @return 如果节点存在且成功执行则返回 true，否则返回 false
     */
    boolean isSourceSuccess(String sourceNodeName);

    /**
     * 检查指定的源节点是否执行失败。
     *
     * @param sourceNodeName 源节点名称
     * @return 如果节点存在且状态为 FAILURE 则返回 true，否则返回 false
     */
    boolean isSourceFailure(String sourceNodeName);

    /**
     * 检查指定的源节点是否被跳过。
     *
     * @param sourceNodeName 源节点名称
     * @return 如果节点存在且状态为 SKIPPED 则返回 true，否则返回 false
     */
    boolean isSourceSkipped(String sourceNodeName);

    /**
     * 检查指定的源节点是否存在于结果中。
     *
     * @param sourceNodeName 源节点名称
     * @return 如果结果中包含该源节点则返回 true
     */
    boolean containsSource(String sourceNodeName);

    /**
     * 获取指定源节点的错误信息。
     *
     * @param sourceNodeName 源节点名称
     * @return 包含错误的 Optional
     */
    default Optional<Throwable> getSourceError(String sourceNodeName) {
        return getSourceResult(sourceNodeName).flatMap(NodeResult::getError);
    }
}
