package xyz.vvrf.reactor.dag.impl;

import reactor.core.publisher.Flux;
import xyz.vvrf.reactor.dag.core.DependencyAccessor;
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * DependencyAccessor 的默认实现。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen
 */
public class DefaultDependencyAccessor<C> implements DependencyAccessor<C> {

    private final Map<String, NodeResult<C, ?, ?>> results;

    /**
     * 创建 DefaultDependencyAccessor 实例。
     * @param results 依赖节点的执行结果 Map (不能为空)
     */
    public DefaultDependencyAccessor(Map<String, NodeResult<C, ?, ?>> results) {
        // 允许传入空 Map，但不允许传入 null
        this.results = Objects.requireNonNull(results, "Dependency results map cannot be null");
    }

    @Override
    public Optional<NodeResult<C, ?, ?>> getResult(String dependencyName) {
        return Optional.ofNullable(results.get(dependencyName));
    }

    @Override
    public <DepP> Optional<DepP> getPayload(String dependencyName, Class<DepP> expectedType) {
        Objects.requireNonNull(expectedType, "Expected payload type cannot be null");
        return getResult(dependencyName)          // 获取 Optional<NodeResult>
                .filter(NodeResult::isSuccess)    // 过滤掉执行失败的
                .flatMap(NodeResult::getPayload)  // 获取 Optional<Payload> (类型是 ?)
                .filter(expectedType::isInstance) // 检查 Payload 实例是否匹配期望类型
                .map(expectedType::cast);         // 安全地转换为期望类型 DepP
    }

    @Override
    public Flux<Event<?>> getEvents(String dependencyName) {
        return getResult(dependencyName)
                // 考虑是否只在成功时返回事件流。通常是的。
                // 如果失败也可能产生事件（如错误事件），逻辑需要调整。
                .filter(NodeResult::isSuccess)
                .map(NodeResult::getEvents) // 获取 Flux<Event<?>>
                .orElse(Flux.empty());      // 如果依赖不存在或失败，返回空流
    }

    @Override
    public boolean isSuccess(String dependencyName) {
        return getResult(dependencyName)
                .map(NodeResult::isSuccess) // 映射到成功状态
                .orElse(false);             // 如果依赖不存在，视为不成功
    }

    @Override
    public boolean contains(String dependencyName) {
        return results.containsKey(dependencyName);
    }

    // --- 可选的便捷方法实现示例 ---
    /*
    public Optional<Throwable> getError(String dependencyName) {
        return getResult(dependencyName).flatMap(NodeResult::getError);
    }

    public <DepP> DepP getPayloadOrThrow(String dependencyName, Class<DepP> expectedType) {
        return getPayload(dependencyName, expectedType)
                .orElseThrow(() -> new NoSuchElementException(
                        String.format("No payload of type %s found for successful dependency '%s'",
                                expectedType.getSimpleName(), dependencyName)));
    }

    public <DepP> DepP getPayloadOrDefault(String dependencyName, Class<DepP> expectedType, DepP defaultValue) {
        return getPayload(dependencyName, expectedType).orElse(defaultValue);
    }
    */
}
