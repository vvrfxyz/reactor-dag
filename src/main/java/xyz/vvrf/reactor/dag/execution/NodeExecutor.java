package xyz.vvrf.reactor.dag.execution;

import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.InputAccessor;
import xyz.vvrf.reactor.dag.core.NodeDefinition;
import xyz.vvrf.reactor.dag.core.NodeResult;

/**
 * 节点执行器接口。
 * 负责执行单个节点实例的逻辑，包括查找节点实现、应用策略（超时、重试）、
 * 调用节点方法并处理结果。
 * 实现类通常需要访问 NodeRegistry 来获取节点实现。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen
 */
public interface NodeExecutor<C> {

    /**
     * 执行指定的节点实例。
     *
     * @param nodeDefinition 要执行的节点定义 (不能为空)
     * @param context        当前上下文 (不能为空)
     * @param inputAccessor  由引擎创建并传入的、包含激活输入数据的访问器 (不能为空)
     * @param requestId      当前 DAG 执行的请求 ID (不能为空)
     * @param dagName        当前 DAG 的名称 (不能为空, 用于监控)
     * @return 包含节点执行最终结果的 Mono<NodeResult<C, ?>>。
     * 这个 Mono 要么成功完成并包含一个 NodeResult (SUCCESS, FAILURE, 或 SKIPPED 状态)，
     * 要么失败完成 (onError)，表示执行器本身或节点执行中发生了意外的、未处理的异常。
     * 注意：节点的业务逻辑失败应通过 NodeResult.failure() 返回，而不是让 Mono 失败。
     */
    Mono<NodeResult<C, ?>> executeNode(
            NodeDefinition nodeDefinition,
            C context,
            InputAccessor<C> inputAccessor,
            String requestId,
            String dagName
    );

    /**
     * 获取此执行器关联的上下文类型。
     * 这对于 DagEngineProvider 自动发现和匹配 Executor 非常重要。
     *
     * @return 上下文类型的 Class 对象。
     */
    Class<C> getContextType();
}
