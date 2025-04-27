package xyz.vvrf.reactor.dag.impl;

import reactor.core.publisher.Flux;
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.core.InputDependencyAccessor;
import xyz.vvrf.reactor.dag.core.InputRequirement;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.util.*;
import java.util.stream.Collectors;

/**
 * InputDependencyAccessor 的默认实现。
 * 基于当前执行上下文中 *所有已完成* 节点的累积结果和输入映射来提供数据。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen (Refactored by Devin)
 */
public class DefaultInputDependencyAccessor<C> implements InputDependencyAccessor<C> {

    // 修改：存储所有已完成节点的结果，而不仅仅是直接前驱
    private final Map<String, NodeResult<C, ?, ?>> allCompletedResults;
    private final Map<InputRequirement<?>, String> inputSourceMap; // 当前节点的输入配置映射
    private final String currentNodeName;
    private final String dagName;

    /**
     * 创建 DefaultInputDependencyAccessor 实例。
     *
     * @param allCompletedResults 当前 DAG 执行中所有已完成（成功、失败或跳过）节点的名称到其 NodeResult 的映射 (不能为空)
     * @param inputSourceMap      当前节点已解析（显式或自动）的输入需求到源节点名称的映射 (不能为空)
     * @param currentNodeName     正在执行或准备执行的当前节点的名称
     * @param dagName             DAG 的名称
     */
    public DefaultInputDependencyAccessor(Map<String, NodeResult<C, ?, ?>> allCompletedResults,
                                          Map<InputRequirement<?>, String> inputSourceMap,
                                          String currentNodeName,
                                          String dagName) {
        // 使用不可变视图确保安全
        this.allCompletedResults = Collections.unmodifiableMap(new HashMap<>(
                Objects.requireNonNull(allCompletedResults, "All completed results map cannot be null")));
        this.inputSourceMap = Collections.unmodifiableMap(new HashMap<>(
                Objects.requireNonNull(inputSourceMap, "Input source map cannot be null")));
        this.currentNodeName = Objects.requireNonNull(currentNodeName, "Current node name cannot be null");
        this.dagName = Objects.requireNonNull(dagName, "DAG name cannot be null");
    }

    @Override
    public <DepP> DepP getRequiredPayload(Class<DepP> expectedType) {
        return getPayload(InputRequirement.require(expectedType))
                .orElseThrow(() -> new NoSuchElementException(
                        formatError("Required input", InputRequirement.require(expectedType), "not available or source failed/skipped/not executed yet")));
    }

    @Override
    public <DepP> DepP getRequiredPayload(Class<DepP> expectedType, String qualifier) {
        return getPayload(InputRequirement.require(expectedType, qualifier))
                .orElseThrow(() -> new NoSuchElementException(
                        formatError("Required input", InputRequirement.require(expectedType, qualifier), "not available or source failed/skipped/not executed yet")));
    }

    @Override
    public <DepP> Optional<DepP> getOptionalPayload(Class<DepP> expectedType) {
        // 对于 Optional 输入，我们不希望在歧义时抛出异常，而是返回 empty
        // 歧义检查现在不那么直接，因为源可能不是直接前驱
        // 假设 Definition 验证阶段处理了显式映射的歧义
        // 自动解析的歧义在 Definition 验证时也应处理
        return getPayload(InputRequirement.optional(expectedType));
    }

    @Override
    public <DepP> Optional<DepP> getOptionalPayload(Class<DepP> expectedType, String qualifier) {
        // 带限定符的 Optional 输入不应该有歧义问题（假设映射是唯一的）
        return getPayload(InputRequirement.optional(expectedType, qualifier));
    }

    @Override
    public <DepP> Optional<DepP> getPayload(InputRequirement<DepP> requirement) {
        Objects.requireNonNull(requirement, "InputRequirement cannot be null");

        // 1. 查找映射的源节点名称
        String sourceNodeName = inputSourceMap.get(requirement);

        if (sourceNodeName == null) {
            // 如果映射中没有（包括自动解析失败的情况），则无法满足
            if (!requirement.isOptional()) {
                // 理论上 Definition 验证阶段应该捕获必需输入无映射的情况
                throw new NoSuchElementException(formatError("Required input", requirement, "has no configured or auto-resolved source mapping"));
            } else {
                return Optional.empty(); // 可选输入未找到源映射
            }
        }

        // 2. 从所有已完成的结果中查找源节点的结果
        NodeResult<C, ?, ?> sourceResult = allCompletedResults.get(sourceNodeName);

        if (sourceResult == null) {
            // 源节点结果不存在于已完成结果中，意味着它尚未执行或DAG结构有问题
            if (!requirement.isOptional()) {
                throw new NoSuchElementException(formatError("Required input", requirement, "source node '" + sourceNodeName + "' has not completed execution yet"));
            } else {
                return Optional.empty(); // 可选输入，源未完成
            }
        }

        // 3. 检查源节点状态
        if (!sourceResult.isSuccess()) {
            // 源节点执行失败或被跳过
            if (!requirement.isOptional()) {
                throw new NoSuchElementException(formatError("Required input", requirement, "source node '" + sourceNodeName + "' did not succeed (status: " + sourceResult.getStatus() + ")"));
            } else {
                return Optional.empty(); // 可选输入，源失败或跳过
            }
        }

        // 4. 提取并转换 Payload
        return sourceResult.getPayload()
                // 确保 Payload 类型匹配需求 (防御性检查)
                .filter(payload -> requirement.getType().isInstance(payload))
                .map(payload -> requirement.getType().cast(payload));
    }


    @Override
    public Flux<Event<?>> getAllAvailableEvents() {
        // 从所有已成功的节点结果中合并事件流
        List<Flux<Event<?>>> eventFluxes = allCompletedResults.values().stream()
                .filter(NodeResult::isSuccess)
                .map(result -> {
                    // 需要类型转换，因为 result.getEvents() 返回 Flux<Event<T>>
                    @SuppressWarnings("unchecked")
                    Flux<Event<?>> events = (Flux<Event<?>>) (Flux<? extends Event<?>>) result.getEvents();
                    return events;
                })
                .collect(Collectors.toList());
        return Flux.merge(eventFluxes);
    }

    @Override
    public Optional<NodeResult<C, ?, ?>> getSourceResult(String sourceNodeName) {
        // 从所有已完成的结果中查找
        return Optional.ofNullable(allCompletedResults.get(sourceNodeName));
    }

    @Override
    public boolean isSourceSuccess(String sourceNodeName) {
        return getSourceResult(sourceNodeName).map(NodeResult::isSuccess).orElse(false);
    }

    @Override
    public boolean isSourceFailure(String sourceNodeName) {
        return getSourceResult(sourceNodeName).map(NodeResult::isFailure).orElse(false);
    }

    @Override
    public boolean isSourceSkipped(String sourceNodeName) {
        return getSourceResult(sourceNodeName).map(NodeResult::isSkipped).orElse(false);
    }

    @Override
    public boolean containsSource(String sourceNodeName) {
        // 检查是否存在于已完成的结果中
        return allCompletedResults.containsKey(sourceNodeName);
    }

    private String formatError(String inputNature, InputRequirement<?> requirement, String reason) {
        return String.format("[%s] DAG '%s': Node '%s' - %s %s: %s",
                this.dagName, this.currentNodeName, inputNature, requirement, reason);
    }
}
