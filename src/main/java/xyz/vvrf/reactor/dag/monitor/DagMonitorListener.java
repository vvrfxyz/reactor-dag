package xyz.vvrf.reactor.dag.monitor;

import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.time.Duration;
import java.util.Map;

/**
 * 用于监控 DAG 执行事件的监听器接口。
 * 包括 DAG 级别和节点级别的事件。
 *
 * @author ruifeng.wen
 */
public interface DagMonitorListener {

    /**
     * DAG 执行开始时调用。
     *
     * @param requestId       请求 ID
     * @param dagName         DAG 名称
     * @param dagDefinition   DAG 定义信息
     * @param initialContext  初始上下文对象 (注意: 实现者应考虑处理此对象中的敏感信息)
     */
    void onDagStart(String requestId, String dagName, DagDefinition<?> dagDefinition, Object initialContext);

    /**
     * DAG 执行完成时调用 (无论成功或失败)。
     *
     * @param requestId       请求 ID
     * @param dagName         DAG 名称
     * @param dagDefinition   DAG 定义信息
     * @param totalDuration   DAG 总执行耗时
     * @param success         DAG 是否整体成功
     * @param finalResults    所有节点的最终结果 Map (实例名 -> NodeResult)
     * @param error           如果 DAG 执行因未捕获的顶层异常而失败，则为该异常；否则为 null
     */
    void onDagComplete(String requestId, String dagName, DagDefinition<?> dagDefinition, Duration totalDuration, boolean success, Map<String, NodeResult<?, ?>> finalResults, Throwable error);

    /**
     * 节点执行开始时调用。
     *
     * @param requestId    请求 ID
     * @param dagName      DAG 名称
     * @param instanceName 节点实例名
     * @param node         节点实现
     */
    void onNodeStart(String requestId, String dagName, String instanceName, DagNode<?, ?> node);

    /**
     * 节点成功执行完成时调用。
     *
     * @param requestId     请求 ID
     * @param dagName       DAG 名称
     * @param instanceName  节点实例名
     * @param totalDuration 节点总执行耗时 (包括重试、排队等)
     * @param logicDuration 节点核心逻辑执行耗时
     * @param result        节点结果
     * @param node          节点实现
     */
    void onNodeSuccess(String requestId, String dagName, String instanceName, Duration totalDuration, Duration logicDuration, NodeResult<?, ?> result, DagNode<?, ?> node);

    /**
     * 节点执行失败时调用。
     *
     * @param requestId     请求 ID
     * @param dagName       DAG 名称
     * @param instanceName  节点实例名
     * @param totalDuration 节点总执行耗时
     * @param logicDuration 节点核心逻辑执行耗时 (如果适用)
     * @param error         导致失败的错误
     * @param node          节点实现
     */
    void onNodeFailure(String requestId, String dagName, String instanceName, Duration totalDuration, Duration logicDuration, Throwable error, DagNode<?, ?> node);

    /**
     * 节点被跳过时调用。
     *
     * @param requestId    请求 ID
     * @param dagName      DAG 名称
     * @param instanceName 节点实例名
     * @param node         节点实现 (可能为 null，如果节点定义本身有问题)
     */
    void onNodeSkipped(String requestId, String dagName, String instanceName, DagNode<?, ?> node);

    /**
     * 节点执行超时时调用。
     * 这通常是 onNodeFailure 的一种特定情况，但为方便监控单独列出。
     *
     * @param requestId    请求 ID
     * @param dagName      DAG 名称
     * @param instanceName 节点实例名
     * @param timeout      配置的超时时长
     * @param node         节点实现
     */
    void onNodeTimeout(String requestId, String dagName, String instanceName, Duration timeout, DagNode<?, ?> node);

}