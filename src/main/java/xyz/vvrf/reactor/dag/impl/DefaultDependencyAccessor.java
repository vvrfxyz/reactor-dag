package xyz.vvrf.reactor.dag.impl;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DependencyAccessor;
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.util.*;

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
        return getResult(dependencyName)
                .filter(NodeResult::isSuccess)
                .flatMap(NodeResult::getPayload)
                .filter(expectedType::isInstance)
                .map(expectedType::cast);
    }

    @Override
    public Flux<Event<?>> getEvents(String dependencyName) {
        return Mono.justOrEmpty(getResult(dependencyName))
                .filter(NodeResult::isSuccess)
                .flatMapMany(NodeResult::getEvents);
    }

    @Override
    public boolean isSuccess(String dependencyName) {
        return getResult(dependencyName)
                .map(NodeResult::isSuccess)
                .orElse(false);
    }

    @Override
    public boolean isFailure(String dependencyName) {
        return getResult(dependencyName)
                .map(NodeResult::isFailure)
                .orElse(false);
    }

    @Override
    public boolean isSkipped(String dependencyName) {
        return getResult(dependencyName)
                .map(NodeResult::isSkipped)
                .orElse(false);
    }

    @Override
    public boolean contains(String dependencyName) {
        return results.containsKey(dependencyName);
    }

}
