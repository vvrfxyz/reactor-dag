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
 * 基于预先计算好的上游节点结果和输入映射来提供数据。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen
 */
public class DefaultInputDependencyAccessor<C> implements InputDependencyAccessor<C> {

    private final Map<String, NodeResult<C, ?, ?>> predecessorResults;
    private final Map<InputRequirement<?>, String> inputSourceMap;
    private final String currentNodeName;
    private final String dagName;

    /**
     * 创建 DefaultInputDependencyAccessor 实例。
     *
     * @param predecessorResults 所有直接执行前驱节点的名称到其 NodeResult 的映射 (不能为空)
     * @param inputSourceMap     当前节点已解析（显式或自动）的输入需求到源节点名称的映射 (不能为空)
     * @param currentNodeName    正在执行的当前节点的名称
     * @param dagName            DAG 的名称
     */
    public DefaultInputDependencyAccessor(Map<String, NodeResult<C, ?, ?>> predecessorResults,
                                          Map<InputRequirement<?>, String> inputSourceMap,
                                          String currentNodeName,
                                          String dagName) {
        this.predecessorResults = Objects.requireNonNull(predecessorResults, "Predecessor results map cannot be null");
        this.inputSourceMap = Objects.requireNonNull(inputSourceMap, "Input source map cannot be null");
        this.currentNodeName = Objects.requireNonNull(currentNodeName, "Current node name cannot be null");
        this.dagName = Objects.requireNonNull(dagName, "DAG name cannot be null");
    }

    @Override
    public <DepP> DepP getRequiredPayload(Class<DepP> expectedType) {
        return getPayload(InputRequirement.require(expectedType))
                .orElseThrow(() -> new NoSuchElementException(
                        formatError("Required input", InputRequirement.require(expectedType), "not available or source failed/skipped")));
    }

    @Override
    public <DepP> DepP getRequiredPayload(Class<DepP> expectedType, String qualifier) {
        return getPayload(InputRequirement.require(expectedType, qualifier))
                .orElseThrow(() -> new NoSuchElementException(
                        formatError("Required input", InputRequirement.require(expectedType, qualifier), "not available or source failed/skipped")));
    }

    @Override
    public <DepP> Optional<DepP> getOptionalPayload(Class<DepP> expectedType) {
        // 对于 Optional 输入，我们不希望在歧义时抛出异常，而是返回 empty
        try {
            return getPayload(InputRequirement.optional(expectedType));
        } catch (IllegalStateException e) {
            // 捕获由 getPayload(req) 内部的 checkAmbiguity 抛出的歧义异常
            System.err.printf("[%s] DAG '%s': Node '%s' - Optional input %s has ambiguity, returning empty.%n",
                    dagName, currentNodeName, InputRequirement.optional(expectedType));
            return Optional.empty();
        }
    }

    @Override
    public <DepP> Optional<DepP> getOptionalPayload(Class<DepP> expectedType, String qualifier) {
        // 带限定符的 Optional 输入不应该有歧义问题
        return getPayload(InputRequirement.optional(expectedType, qualifier));
    }

    @Override
    public <DepP> Optional<DepP> getPayload(InputRequirement<DepP> requirement) {
        Objects.requireNonNull(requirement, "InputRequirement cannot be null");

        String sourceNodeName = inputSourceMap.get(requirement);

        if (sourceNodeName == null) {
            // 尝试自动解析（如果无限定符） - 这部分逻辑其实应该在 Definition 的验证阶段完成并填充 inputSourceMap
            // 这里假设 inputSourceMap 已经包含了所有能满足的映射（显式或自动解析的）
            // 如果这里还是 null，说明确实无法满足
            if (!requirement.isOptional()) {
                throw new NoSuchElementException(formatError("Required input", requirement, "has no configured or auto-resolved source"));
            } else {
                return Optional.empty(); // 可选输入未找到源
            }
        }

        NodeResult<C, ?, ?> sourceResult = predecessorResults.get(sourceNodeName);

        if (sourceResult == null) {
            // 源节点结果不存在（理论上不应发生，因为 predecessorResults 包含所有前驱）
            if (!requirement.isOptional()) {
                throw new NoSuchElementException(formatError("Required input", requirement, "source node '" + sourceNodeName + "' result not found in predecessors"));
            } else {
                return Optional.empty();
            }
        }

        if (!sourceResult.isSuccess()) {
            // 源节点执行失败或被跳过
            if (!requirement.isOptional()) {
                throw new NoSuchElementException(formatError("Required input", requirement, "source node '" + sourceNodeName + "' did not succeed (status: " + sourceResult.getStatus() + ")"));
            } else {
                return Optional.empty();
            }
        }

        return sourceResult.getPayload()
                .filter(payload -> requirement.getType().isInstance(payload))
                .map(payload -> requirement.getType().cast(payload));
    }


    @Override
    public Flux<Event<?>> getAllUpstreamEvents() {
        List<Flux<Event<?>>> eventFluxes = predecessorResults.values().stream()
                .filter(NodeResult::isSuccess)
                .map(result -> {
                    @SuppressWarnings("unchecked")
                    Flux<Event<?>> events = (Flux<Event<?>>) (Flux<? extends Event<?>>) result.getEvents();
                    return events;
                })
                .collect(Collectors.toList());
        return Flux.merge(eventFluxes);
    }

    @Override
    public Optional<NodeResult<C, ?, ?>> getSourceResult(String sourceNodeName) {
        return Optional.ofNullable(predecessorResults.get(sourceNodeName));
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
        return predecessorResults.containsKey(sourceNodeName);
    }

    private String formatError(String inputNature, InputRequirement<?> requirement, String reason) {
        return String.format("[%s] DAG '%s': Node '%s' - %s %s: %s",
                dagName, currentNodeName, inputNature, requirement, reason);
    }
}
