package xyz.vvrf.reactor.dag.execution; // 放在 execution 包

import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * LocalInputAccessor 的默认实现。
 * 提供对目标下游节点的所有直接上游节点结果的访问。
 *
 * @param <C> 上下文类型
 */
@Slf4j
public class DefaultLocalInputAccessor<C> implements LocalInputAccessor<C> {

    private final Map<String, NodeResult<C, ?>> completedResults; // 全局结果引用
    private final String targetDownstreamNode;      // 当前评估条件的目标下游节点
    private final Set<String> directUpstreamNames;  // 预先计算好的直接上游节点名称集合

    /**
     * 创建 DefaultLocalInputAccessor 实例。
     *
     * @param completedResults     图中所有已完成节点的结果 Map (不能为空)
     * @param dagDefinition        DAG 定义，用于查找入边 (不能为空)
     * @param targetDownstreamNode 当前评估条件的目标下游节点的实例名称 (不能为空)
     */
    public DefaultLocalInputAccessor(
            Map<String, NodeResult<C, ?>> completedResults,
            DagDefinition<C> dagDefinition,
            String targetDownstreamNode) {

        this.completedResults = Objects.requireNonNull(completedResults, "已完成结果 Map 不能为空");
        Objects.requireNonNull(dagDefinition, "DAG 定义不能为空");
        this.targetDownstreamNode = Objects.requireNonNull(targetDownstreamNode, "目标下游节点名称不能为空");

        // 预计算直接上游节点名称
        this.directUpstreamNames = Collections.unmodifiableSet(
                dagDefinition.getIncomingEdges(targetDownstreamNode).stream()
                        .map(EdgeDefinition::getUpstreamInstanceName)
                        .collect(Collectors.toSet())
        );

        log.trace("为下游节点 '{}' 创建 DefaultLocalInputAccessor，直接上游: {}",
                targetDownstreamNode, directUpstreamNames);
    }

    @Override
    public Optional<NodeResult<C, ?>> getUpstreamResult(String upstreamInstanceName) {
        // 检查请求的节点是否确实是直接上游
        if (!directUpstreamNames.contains(upstreamInstanceName)) {
            log.warn("LocalInputCondition 尝试访问非直接上游节点 '{}' (目标下游: {}, 直接上游: {})",
                    upstreamInstanceName, targetDownstreamNode, directUpstreamNames);
            // 或者抛出异常？当前选择返回 empty
            return Optional.empty();
        }
        // 从全局结果中获取
        return Optional.ofNullable(completedResults.get(upstreamInstanceName));
    }

    @Override
    public Set<String> getDirectUpstreamNames() {
        return directUpstreamNames; // 返回预计算的不可变集合
    }

    // 默认实现的 isUpstreamSuccess 和 getUpstreamPayload 可以直接使用接口中的 default 方法
}
