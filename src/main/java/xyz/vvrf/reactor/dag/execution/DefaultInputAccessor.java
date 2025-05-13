package xyz.vvrf.reactor.dag.execution;

import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * InputAccessor 的默认实现。
 * 基于激活的入边提供的上游结果构建。此类是不可变的。
 * 支持扇入场景，一个输入槽可以从多个上游接收数据。
 *
 * @param <C> 上下文类型
 * @author Ruifeng.wen
 */
@Slf4j
public class DefaultInputAccessor<C> implements InputAccessor<C> {

    // 存储每个 InputSlot 对应的、来自所有激活且成功的上游节点的结果列表
    // Key: InputSlot<?> 实例 (来自节点定义)
    // Value: List<NodeResult<C, ?>> (来自上游成功执行的结果列表)
    private final Map<InputSlot<?>, List<NodeResult<C, ?>>> activeSuccessfulUpstreamResultsPerSlot;

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
     * @param nodeInputSlots      当前节点声明的所有 InputSlot 集合 (不能为空)
     * @param incomingEdges       连接到当前节点的所有 EdgeDefinition 列表 (不能为空)
     * @param completedResults    图中所有已完成节点的结果 Map (InstanceName -> NodeResult) (不能为空)
     * @param activeIncomingEdges 评估后确定为激活状态的入边列表 (不能为空)
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

        // 本地可变 Map，用于在构造函数中构建数据
        Map<InputSlot<?>, List<NodeResult<C, ?>>> successfulResultsMapLocal = new HashMap<>();
        Map<InputSlot<?>, List<EdgeInfo<C>>> edgesMapLocal = new HashMap<>();
        Set<EdgeDefinition<C>> activeEdgeSet = new HashSet<>(activeIncomingEdges); // 用于快速查找激活状态

        // 1. 构建 edgeInfoMap 和 successfulResultsMapLocal
        for (EdgeDefinition<C> edge : incomingEdges) {
            InputSlot<?> targetSlot = findInputSlotById(nodeInputSlots, edge.getInputSlotId());
            if (targetSlot == null) {
                log.warn("在为 InputAccessor 构建时，找不到入边 {} 指向的输入槽 '{}'。忽略此边。", edge, edge.getInputSlotId());
                continue;
            }

            NodeResult<C, ?> upstreamResult = completedResults.get(edge.getUpstreamInstanceName());
            if (upstreamResult == null) {
                log.error("严重错误：在为 InputAccessor 构建时，找不到上游节点 '{}' 的结果。下游槽: {}",
                        edge.getUpstreamInstanceName(), targetSlot);
                continue;
            }

            boolean isActive = activeEdgeSet.contains(edge);
            EdgeInfo<C> info = new EdgeInfo<>(edge.getUpstreamInstanceName(), upstreamResult, isActive);

            // 添加到 edgeInfoMap
            edgesMapLocal.computeIfAbsent(targetSlot, k -> new ArrayList<>()).add(info);

            // 如果边是激活的且上游成功，添加到 successfulResultsMapLocal
            if (isActive && upstreamResult.isSuccess()) {
                successfulResultsMapLocal.computeIfAbsent(targetSlot, k -> new ArrayList<>()).add(upstreamResult);
                log.trace("为输入槽 '{}' 添加了来自上游 '{}' 的可用负载到列表。", targetSlot.getId(), edge.getUpstreamInstanceName());
            }
        }

        // 转换为不可变 Map，内部 List 也转换为不可变
        Map<InputSlot<?>, List<NodeResult<C, ?>>> finalSuccessfulResults = new HashMap<>();
        successfulResultsMapLocal.forEach((slot, resultList) ->
                finalSuccessfulResults.put(slot, Collections.unmodifiableList(new ArrayList<>(resultList)))
        );
        this.activeSuccessfulUpstreamResultsPerSlot = Collections.unmodifiableMap(finalSuccessfulResults);

        Map<InputSlot<?>, List<EdgeInfo<C>>> finalEdgesMap = new HashMap<>();
        edgesMapLocal.forEach((slot, infoList) ->
                finalEdgesMap.put(slot, Collections.unmodifiableList(new ArrayList<>(infoList)))
        );
        this.edgeInfoMap = Collections.unmodifiableMap(finalEdgesMap);
    }

    // 辅助方法：根据 ID 查找 InputSlot
    private InputSlot<?> findInputSlotById(Set<InputSlot<?>> slots, String id) {
        for (InputSlot<?> slot : slots) {
            if (slot.getId().equals(id)) {
                return slot;
            }
        }
        return null;
    }


    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getPayload(InputSlot<T> inputSlot) {
        List<NodeResult<C, ?>> results = activeSuccessfulUpstreamResultsPerSlot.get(inputSlot);
        if (results != null && !results.isEmpty()) {
            for (NodeResult<C, ?> result : results) {
                // result is already guaranteed to be from a successful and active upstream
                Optional<T> payload = result.getPayload()
                        .filter(inputSlot.getType()::isInstance)
                        .map(p -> (T) p);
                if (payload.isPresent()) {
                    return payload; // Return the first valid payload found
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public <T> List<Optional<T>> getAllPayloads(InputSlot<T> inputSlot) {
        List<NodeResult<C, ?>> results = activeSuccessfulUpstreamResultsPerSlot.get(inputSlot);
        if (results == null || results.isEmpty()) {
            return Collections.emptyList();
        }
        // Type casting for p -> (T) p is safe due to filter(inputSlot.getType()::isInstance)
        return results.stream()
                .map(result -> result.getPayload()
                        .filter(inputSlot.getType()::isInstance)
                        .map(p -> (T) p))
                .collect(Collectors.toList());
    }

    @Override
    public boolean isAvailable(InputSlot<?> inputSlot) {
        List<NodeResult<C, ?>> results = activeSuccessfulUpstreamResultsPerSlot.get(inputSlot);
        if (results != null && !results.isEmpty()) {
            // Check if any result in the list has a payload that matches the slot's type
            return results.stream()
                    .anyMatch(result -> result.getPayload()
                            .filter(inputSlot.getType()::isInstance)
                            .isPresent());
        }
        return false;
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
        // 如果 activeSuccessfulUpstreamResultsPerSlot 中没有该槽的条目，
        // 或者条目对应的列表为空（即没有激活且成功的上游提供数据），
        // 并且 edgeInfoMap 中存在该槽（即至少有一条边定义连接到它），
        // 则认为它是非激活的（或上游未成功，或条件不满足等）。
        List<NodeResult<C, ?>> successfulResults = activeSuccessfulUpstreamResultsPerSlot.get(inputSlot);
        boolean hasAnySuccessfulActiveInput = successfulResults != null && !successfulResults.isEmpty();

        return edgeInfoMap.containsKey(inputSlot) && !hasAnySuccessfulActiveInput;
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
