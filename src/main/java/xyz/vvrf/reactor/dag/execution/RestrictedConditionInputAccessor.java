package xyz.vvrf.reactor.dag.execution;

import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.ConditionInputAccessor;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.util.*;

/**
 * ConditionInputAccessor 的一个受限实现。
 * 只允许访问预先定义的一组节点实例的结果。
 * 由 StandardDagEngine 在评估 DeclaredDependencyCondition 时创建和使用。
 *
 * @param <C> 上下文类型
 */
@Slf4j
public class RestrictedConditionInputAccessor<C> implements ConditionInputAccessor<C> {

    private final Map<String, NodeResult<C, ?>> completedResults; // 全局结果引用
    private final Set<String> allowedNodeNames;       // 允许访问的节点名称集合
    private final String directUpstreamName;          // 当前边的直接上游节点名 (用于日志和错误消息)

    /**
     * 创建受限的访问器。
     *
     * @param completedResults   图中所有已完成节点的结果 Map (不能为空)
     * @param allowedNodeNames   允许访问的节点实例名称集合 (不能为空, 必须包含 directUpstreamName)
     * @param directUpstreamName 当前评估边的直接上游节点实例名称 (不能为空)
     */
    public RestrictedConditionInputAccessor(
            Map<String, NodeResult<C, ?>> completedResults,
            Set<String> allowedNodeNames,
            String directUpstreamName) {
        this.completedResults = Objects.requireNonNull(completedResults, "已完成结果 Map 不能为空");
        this.allowedNodeNames = Objects.requireNonNull(allowedNodeNames, "允许访问的节点名称集合不能为空");
        this.directUpstreamName = Objects.requireNonNull(directUpstreamName, "直接上游节点名称不能为空");

        if (!allowedNodeNames.contains(directUpstreamName)) {
            throw new IllegalArgumentException(String.format(
                    "内部错误：允许访问的节点集合 %s 必须包含直接上游节点 '%s'",
                    allowedNodeNames, directUpstreamName));
        }
        log.trace("创建 RestrictedConditionInputAccessor: 允许访问 {}, 直接上游: {}", allowedNodeNames, directUpstreamName);
    }

    private void checkAccess(String instanceName) {
        if (!allowedNodeNames.contains(instanceName)) {
            log.warn("条件试图访问未声明或不允许的节点 '{}' (允许: {}, 直接上游: {})。请检查 DeclaredDependencyCondition 实现的 getRequiredNodeDependencies()。",
                    instanceName, allowedNodeNames, directUpstreamName);
            // 抛出异常以强制执行限制
            throw new IllegalArgumentException(String.format(
                    "条件不允许访问节点 '%s' 的结果。允许访问的节点：%s (直接上游: %s)。请通过 getRequiredNodeDependencies() 声明依赖。",
                    instanceName, allowedNodeNames, directUpstreamName));
        }
    }

    @Override
    public Optional<NodeResult.NodeStatus> getNodeStatus(String instanceName) {
        checkAccess(instanceName); // 强制检查
        return Optional.ofNullable(completedResults.get(instanceName)).map(NodeResult::getStatus);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> Optional<P> getNodePayload(String instanceName, Class<P> expectedType) {
        checkAccess(instanceName); // 强制检查
        return Optional.ofNullable(completedResults.get(instanceName))
                .filter(NodeResult::isSuccess)
                .flatMap(NodeResult::getPayload)
                .filter(expectedType::isInstance)
                .map(p -> (P) p);
    }

    @Override
    public Optional<Throwable> getNodeError(String instanceName) {
        checkAccess(instanceName); // 强制检查
        return Optional.ofNullable(completedResults.get(instanceName))
                .filter(NodeResult::isFailure)
                .flatMap(NodeResult::getError);
    }

    @Override
    public Set<String> getCompletedNodeNames() {
        // 返回允许访问的节点中，实际已完成的节点名称
        Set<String> completedAllowed = new HashSet<>(allowedNodeNames);
        completedAllowed.retainAll(completedResults.keySet());
        return Collections.unmodifiableSet(completedAllowed);
    }
}
