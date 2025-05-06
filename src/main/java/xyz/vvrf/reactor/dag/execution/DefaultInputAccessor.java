package xyz.vvrf.reactor.dag.execution;

import xyz.vvrf.reactor.dag.core.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * InputAccessor 的默认实现。
 * 基于激活的入边提供的上游结果构建。
 *
 * @param <C> 上下文类型
 * @author Refactored
 */
public class DefaultInputAccessor<C> implements InputAccessor<C> {

    // 当前节点声明的所有输入槽 -> 连接到该槽的上游结果 (如果边激活且上游完成)
    private final Map<InputSlot<?>, NodeResult<C, ?>> activeInputs;
    // 当前节点声明的所有输入槽 -> 连接到该槽的边定义 (用于检查状态)
    private final Map<InputSlot<?>, EdgeDefinition<C>> edgeMap;
    // 所有已完成的上游节点结果 (用于检查 failed/skipped 状态)
    private final Map<String, NodeResult<C, ?>> allUpstreamResults;


    /**
     * 创建 DefaultInputAccessor 实例。
     *
     * @param nodeInputSlots        当前节点声明的所有 InputSlot
     * @param incomingEdges         连接到当前节点的所有 EdgeDefinition
     * @param completedResults      图中所有已完成节点的结果 (InstanceName -> NodeResult)
     * @param activeIncomingEdges   评估后确定为激活状态的入边列表
     */
    public DefaultInputAccessor(
            Set<InputSlot<?>> nodeInputSlots,
            List<EdgeDefinition<C>> incomingEdges,
            Map<String, NodeResult<C, ?>> completedResults,
            List<EdgeDefinition<C>> activeIncomingEdges) {

        Objects.requireNonNull(nodeInputSlots, "Node input slots cannot be null");
        Objects.requireNonNull(incomingEdges, "Incoming edges cannot be null");
        Objects.requireNonNull(completedResults, "Completed results cannot be null");
        Objects.requireNonNull(activeIncomingEdges, "Active incoming edges cannot be null");

        this.allUpstreamResults = completedResults; // 引用，不需要复制

        // 构建 edgeMap: InputSlot -> EdgeDefinition (假设一个槽只连接一个边，或取第一个？需要明确策略)
        // 当前假设：一个 InputSlot 可能由多个 Edge 连接，但 InputAccessor 只关心是否有 *至少一个* 激活的边提供了数据。
        // 状态检查 (isFailed, isSkipped) 需要知道连接关系。
        // 简化：暂时假设一个 InputSlot 只被一个 Edge 连接，或只关心第一个。
        // TODO: 重新审视多对一连接的处理策略
        this.edgeMap = incomingEdges.stream()
                .collect(Collectors.toMap(
                        edge -> findInputSlotById(nodeInputSlots, edge.getInputSlotId()),
                        edge -> edge,
                        (existing, replacement) -> {
                            // 如果一个输入槽被多个边连接，这里需要合并策略，暂时取第一个
                            // log.warn("Input slot '{}' is connected by multiple edges. Using the first one for status checks.", existing.getInputSlotId());
                            return existing;
                        }
                ));


        // 构建 activeInputs: InputSlot -> NodeResult (只包含激活边的数据)
        Map<InputSlot<?>, NodeResult<C, ?>> inputs = new ConcurrentHashMap<>();
        for (EdgeDefinition<C> activeEdge : activeIncomingEdges) {
            NodeResult<C, ?> upstreamResult = completedResults.get(activeEdge.getUpstreamInstanceName());
            if (upstreamResult != null && upstreamResult.isSuccess()) { // 必须是成功的上游
                InputSlot<?> targetSlot = findInputSlotById(nodeInputSlots, activeEdge.getInputSlotId());
                if (targetSlot != null) {
                    // TODO: 如果一个槽被多个激活边连接，如何处理？取第一个？合并？
                    // 暂时：后面的覆盖前面的
                    inputs.put(targetSlot, upstreamResult);
                }
            }
        }
        this.activeInputs = Collections.unmodifiableMap(inputs);
    }

    private InputSlot<?> findInputSlotById(Set<InputSlot<?>> slots, String id) {
        return slots.stream().filter(s -> s.getId().equals(id)).findFirst().orElse(null);
    }


    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getPayload(InputSlot<T> inputSlot) {
        NodeResult<C, ?> result = activeInputs.get(inputSlot);
        if (result != null && result.isSuccess()) {
            // 类型应该在构建时已验证匹配，但这里可以加一层保险
            return result.getPayload()
                    .filter(p -> inputSlot.getType().isInstance(p))
                    .map(p -> (T) p); // 强制转换是安全的，因为类型已检查
        }
        return Optional.empty();
    }

    @Override
    public boolean isAvailable(InputSlot<?> inputSlot) {
        // 检查 activeInputs 中是否有对应槽，并且其 Payload 存在
        return activeInputs.containsKey(inputSlot) && activeInputs.get(inputSlot).getPayload().isPresent();
    }

    @Override
    public boolean isFailed(InputSlot<?> inputSlot) {
        EdgeDefinition<C> edge = edgeMap.get(inputSlot);
        if (edge == null) return false; // 未连接
        NodeResult<C, ?> upstreamResult = allUpstreamResults.get(edge.getUpstreamInstanceName());
        return upstreamResult != null && upstreamResult.isFailure();
    }

    @Override
    public boolean isSkipped(InputSlot<?> inputSlot) {
        EdgeDefinition<C> edge = edgeMap.get(inputSlot);
        if (edge == null) return false; // 未连接
        NodeResult<C, ?> upstreamResult = allUpstreamResults.get(edge.getUpstreamInstanceName());
        return upstreamResult != null && upstreamResult.isSkipped();
    }

    @Override
    public boolean isInactive(InputSlot<?> inputSlot) {
        // 如果不在 activeInputs 中，但在 edgeMap 中，则说明边未激活或上游未成功
        return edgeMap.containsKey(inputSlot) && !activeInputs.containsKey(inputSlot);
    }

    @Override
    public Optional<Throwable> getError(InputSlot<?> inputSlot) {
        EdgeDefinition<C> edge = edgeMap.get(inputSlot);
        if (edge == null) return Optional.empty();
        NodeResult<C, ?> upstreamResult = allUpstreamResults.get(edge.getUpstreamInstanceName());
        if (upstreamResult != null && upstreamResult.isFailure()) {
            return upstreamResult.getError();
        }
        return Optional.empty();
    }
}