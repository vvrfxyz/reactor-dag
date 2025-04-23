package xyz.vvrf.reactor.dag.core;

import reactor.core.publisher.Flux;

import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * 提供对节点依赖执行结果的安全、便捷访问。
 * 这是传递给 DagNode.execute() 方法的参数类型，取代了原始的 Map。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen
 */
public interface DependencyAccessor<C> {

    /**
     * 获取指定依赖节点的完整 NodeResult。
     * 可用于访问上下文、错误信息、事件类型等。
     *
     * @param dependencyName 依赖节点的名称
     * @return 包含 NodeResult 的 Optional，如果依赖不存在或未执行则为空
     */
    Optional<NodeResult<C, ?, ?>> getResult(String dependencyName);

    /**
     * 安全地获取指定依赖节点的 Payload。
     * 内部会检查节点是否存在、是否成功执行、Payload 是否存在以及类型是否匹配。
     *
     * @param <DepP>         期望的 Payload 类型
     * @param dependencyName 依赖节点的名称
     * @param expectedType   期望的 Payload 类型的 Class 对象
     * @return 包含符合类型 Payload 的 Optional，否则为空
     */
    <DepP> Optional<DepP> getPayload(String dependencyName, Class<DepP> expectedType);

    /**
     * 获取指定依赖节点的事件流 Flux<Event<?>>。
     * 注意：返回的 Flux 可能在节点执行时仍然是活动的（流式处理）。
     * 如果依赖节点执行失败或不存在，通常返回 Flux.empty()。
     *
     * @param dependencyName 依赖节点的名称
     * @return 事件流 Flux<Event<?>> (可能为空)
     */
    Flux<Event<?>> getEvents(String dependencyName);

    /**
     * 检查指定的依赖节点是否成功执行。
     *
     * @param dependencyName 依赖节点名称
     * @return 如果节点存在且成功执行则返回 true，否则返回 false
     */
    boolean isSuccess(String dependencyName);

    /**
     * 检查指定的依赖节点是否执行失败。
     *
     * @param dependencyName 依赖节点名称
     * @return 如果节点存在且状态为 FAILURE 则返回 true，否则返回 false
     */
    boolean isFailure(String dependencyName);

    /**
     * 检查指定的依赖节点是否被跳过。
     *
     * @param dependencyName 依赖节点名称
     * @return 如果节点存在且状态为 SKIPPED 则返回 true，否则返回 false
     */
    boolean isSkipped(String dependencyName);


    /**
     * 检查指定的依赖节点是否存在于结果中。
     *
     * @param dependencyName 依赖节点名称
     * @return 如果结果中包含该依赖节点则返回 true
     */
    boolean contains(String dependencyName);

    default Optional<Throwable> getError(String dependencyName) {
        return getResult(dependencyName).flatMap(NodeResult::getError);
    }

    default <DepP> DepP getPayloadOrThrow(String dependencyName, Class<DepP> expectedType) {
        return getPayload(dependencyName, expectedType)
                .orElseThrow(() -> new NoSuchElementException(
                        String.format("No payload of type %s found for successful dependency '%s'",
                                expectedType.getSimpleName(), dependencyName)));
    }

    default <DepP> DepP getPayloadOrDefault(String dependencyName, Class<DepP> expectedType, DepP defaultValue) {
        return getPayload(dependencyName, expectedType).orElse(defaultValue);
    }
}
