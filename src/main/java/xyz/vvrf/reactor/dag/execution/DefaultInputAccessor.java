package xyz.vvrf.reactor.dag.execution;

import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * InputAccessor 的默认实现。
 * 基于激活的入边提供的上游结果构建。此类是不可变的。
 *
 * @param <C> 上下文类型
 * @author Refactored (注释更新)
 */
@Slf4j
public class DefaultInputAccessor<C> implements InputAccessor<C> {

    // 存储每个 InputSlot 对应的激活且成功的上游结果
    // Key: InputSlot<?> 实例 (来自节点定义)
    // Value: NodeResult<C, ?> (来自上游成功执行的结果)
    private final Map<InputSlot<?>, NodeResult<C, ?>> availablePayloads;

    // 存储每个 InputSlot 连接的所有入边的信息 (用于状态检查)
    // Key: InputSlot<?> 实例
    // Value: List<EdgeInfo> (包含上游名称和状态)
    private final Map<InputSlot<?>, List<EdgeInfo<C>>> edgeInfoMap;

    // 内部类，用于存储边的状态信息
    private static class EdgeInfo<C> {
        final String upstreamInstanceName;
        final NodeResult<C, ?> upstreamResult; // 可能为 null 如果上游未完成 (理论上不应发生在此阶段)
        final boolean isActive; // 边条件是否满足

        EdgeInfo(String upstreamInstanceName, NodeResult<C, ?> upstreamResult, boolean isActive) {
            this.upstreamInstanceName = upstreamInstanceName;
            this.upstreamResult = upstreamResult;
            this.isActive = isActive;
        }

        NodeResult.NodeStatus getStatus() {
            return upstreamResult != null ? upstreamResult.getStatus() : null; // 处理理论上的 null 情况
        }

        Optional<Throwable> getError() {
            return upstreamResult != null ? upstreamResult.getError() : Optional.empty();
        }
    }

    /**
     * 创建 DefaultInputAccessor 实例。
     *
     * @param nodeInputSlots        当前节点声明的所有 InputSlot 集合 (不能为空)
     * @param incomingEdges         连接到当前节点的所有 EdgeDefinition 列表 (不能为空)
     * @param completedResults      图中所有已完成节点的结果 Map (InstanceName -> NodeResult) (不能为空)
     * @param activeIncomingEdges   评估后确定为激活状态的入边列表 (不能为空)
     */
    public DefaultInputAccessor(
            Set<InputSlot<?>> nodeInputSlots,
            List<EdgeDefinition<C>> incomingEdges,
            Map<String, NodeResult<C, ?>> completedResults,
            List<EdgeDefinition<C>> activeIncomingEdges) {

        Objects.requireNonNull(nodeInputSlots, "节点输入槽集合不能为空");
        Objects.requireNonNull(incomingEdges, "入边列表不能为空");
        Objects.requireNonNull(completedResults, "已完成结果 Map 不能为空");
        Objects.requireNonNull(activeIncomingEdges, "激活的入边列表不能为空");

        Map<InputSlot<?>, NodeResult<C, ?>> payloads = new ConcurrentHashMap<>();
        Map<InputSlot<?>, List<EdgeInfo<C>>> edges = new HashMap<>();
        Set<EdgeDefinition<C>> activeEdgeSet = new HashSet<>(activeIncomingEdges); // 用于快速查找激活状态

        // 1. 构建 edgeInfoMap 和 availablePayloads
        for (EdgeDefinition<C> edge : incomingEdges) {
            InputSlot<?> targetSlot = findInputSlotById(nodeInputSlots, edge.getInputSlotId());
            if (targetSlot == null) {
                // 这通常不应该发生，因为 DagDefinitionBuilder 会验证
                log.warn("在为 InputAccessor 构建时，找不到入边 {} 指向的输入槽 '{}'。忽略此边。", edge, edge.getInputSlotId());
                continue;
            }

            NodeResult<C, ?> upstreamResult = completedResults.get(edge.getUpstreamInstanceName());
            if (upstreamResult == null) {
                // 这也不应该发生，因为依赖应该已经完成
                log.error("严重错误：在为 InputAccessor 构建时，找不到上游节点 '{}' 的结果。下游槽: {}",
                        edge.getUpstreamInstanceName(), targetSlot);
                // 可以选择抛出异常或继续，这里选择记录错误并继续
                continue;
            }

            boolean isActive = activeEdgeSet.contains(edge);
            EdgeInfo<C> info = new EdgeInfo<>(edge.getUpstreamInstanceName(), upstreamResult, isActive);

            // 添加到 edgeInfoMap
            edges.computeIfAbsent(targetSlot, k -> new ArrayList<>()).add(info);

            // 如果边是激活的且上游成功，尝试放入 availablePayloads
            if (isActive && upstreamResult.isSuccess()) {
                // 处理一个输入槽被多个激活边连接的情况
                // 策略：后面的覆盖前面的（简单策略）。可以根据需要调整为其他策略，如抛异常或合并。
                payloads.put(targetSlot, upstreamResult);
                log.trace("为输入槽 '{}' 设置了来自上游 '{}' 的可用负载。", targetSlot.getId(), edge.getUpstreamInstanceName());
            }
        }

        // 转换为不可变 Map
        this.availablePayloads = Collections.unmodifiableMap(payloads);
        // 将 List 转换为不可变 List
        Map<InputSlot<?>, List<EdgeInfo<C>>> immutableEdges = new HashMap<>();
        edges.forEach((slot, infoList) -> immutableEdges.put(slot, Collections.unmodifiableList(infoList)));
        this.edgeInfoMap = Collections.unmodifiableMap(immutableEdges);
    }

    // 辅助方法：根据 ID 查找 InputSlot
    private InputSlot<?> findInputSlotById(Set<InputSlot<?>> slots, String id) {
        for (InputSlot<?> slot : slots) {
            if (slot.getId().equals(id)) {
                return slot;
            }
        }
        return null; // 或者抛出异常，但理论上 Builder 已验证
    }


    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getPayload(InputSlot<T> inputSlot) {
        NodeResult<C, ?> result = availablePayloads.get(inputSlot);
        if (result != null) { // availablePayloads 只存储成功的
            // 从 NodeResult 中获取 Optional<Payload>
            return result.getPayload()
                    // 验证类型（理论上 Builder 已保证，但作为防御性措施）
                    .filter(p -> inputSlot.getType().isInstance(p))
                    .map(p -> (T) p); // 类型转换是安全的
        }
        return Optional.empty();
    }

    @Override
    public boolean isAvailable(InputSlot<?> inputSlot) {
        // 检查 availablePayloads 中是否有对应槽，并且其 Payload 存在
        // 注意：availablePayloads 只包含来自激活、成功上游的数据
        NodeResult<C, ?> result = availablePayloads.get(inputSlot);
        return result != null && result.getPayload().isPresent();
    }

    @Override
    public boolean isFailed(InputSlot<?> inputSlot) {
        List<EdgeInfo<C>> infos = edgeInfoMap.get(inputSlot);
        if (infos == null || infos.isEmpty()) {
            return false; // 没有边连接到此槽
        }
        // 只要有一个连接的上游失败，就返回 true
        for (EdgeInfo<C> info : infos) {
            if (info.getStatus() == NodeResult.NodeStatus.FAILURE) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSkipped(InputSlot<?> inputSlot) {
        List<EdgeInfo<C>> infos = edgeInfoMap.get(inputSlot);
        if (infos == null || infos.isEmpty()) {
            return false;
        }
        // 只要有一个连接的上游被跳过，就返回 true
        for (EdgeInfo<C> info : infos) {
            if (info.getStatus() == NodeResult.NodeStatus.SKIPPED) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isInactive(InputSlot<?> inputSlot) {
        // 如果不在 availablePayloads 中（即没有激活且成功的上游提供数据），
        // 并且 edgeInfoMap 中存在该槽（即至少有一条边定义连接到它），
        // 则认为它是非激活的（或上游未成功）。
        return !availablePayloads.containsKey(inputSlot) && edgeInfoMap.containsKey(inputSlot);
    }

    @Override
    public Optional<Throwable> getError(InputSlot<?> inputSlot) {
        List<EdgeInfo<C>> infos = edgeInfoMap.get(inputSlot);
        if (infos == null || infos.isEmpty()) {
            return Optional.empty();
        }
        // 返回第一个找到的失败上游的错误
        for (EdgeInfo<C> info : infos) {
            if (info.getStatus() == NodeResult.NodeStatus.FAILURE) {
                return info.getError();
            }
        }
        return Optional.empty();
    }
}
