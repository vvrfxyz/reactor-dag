// [file name]: DefaultDependencyAccessor.java
package xyz.vvrf.reactor.dag.impl;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DependencyAccessor;
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.util.*;

/**
 * DependencyAccessor 的默认实现。
 * 基于存储 NodeResult 的 Map。
 *
 * @param <C> 上下文类型
 */
public class DefaultDependencyAccessor<C> implements DependencyAccessor<C> {

    private final Map<String, NodeResult<C, ?>> results;

    /**
     * 创建 DefaultDependencyAccessor 实例。
     * @param results 依赖节点的执行结果 Map (不能为空)
     */
    public DefaultDependencyAccessor(Map<String, NodeResult<C, ?>> results) {
        // 允许传入空 Map，但不允许传入 null
        this.results = Objects.requireNonNull(results, "Dependency results map cannot be null");
    }

    @Override
    public Optional<NodeResult<C, ?>> getResult(String dependencyName) {
        return Optional.ofNullable(results.get(dependencyName));
    }

    // getPayload 方法已被移除

    @Override
    public Flux<Event<?>> getEvents(String dependencyName) {
        // 从 NodeResult 中获取事件流
        // 如果节点失败或跳过，NodeResult 内部的 events 通常是 Flux.empty()
        return Mono.justOrEmpty(getResult(dependencyName))
                // .filter(NodeResult::isSuccess) // 是否只获取成功节点的事件？取决于需求，暂时获取所有存在的
                .flatMapMany(NodeResult::getEvents); // 直接获取事件流
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

    // getError 方法已在接口中提供默认实现
}
