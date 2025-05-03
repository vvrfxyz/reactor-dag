// file: monitor/DagMonitorListener.java
package xyz.vvrf.reactor.dag.monitor;

import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.time.Duration;

/**
 * 用于监控 DAG 节点执行事件的监听器接口。
 * 注意：现在 nodeName 是指节点实例的名称。
 * DagNode 参数是该实例使用的实现。
 * NodeResult 不再有 EventType。
 *
 * @author ruifeng.wen (Refactored)
 */
public interface DagMonitorListener {

    /**
     * 当一个节点实例即将被执行时调用。
     *
     * @param requestId DAG 执行请求的唯一 ID。
     * @param dagName   正在执行的 DAG 定义的名称。
     * @param instanceName 即将开始的节点实例的名称。
     * @param node      该实例使用的 DagNode 实现。
     */
    void onNodeStart(String requestId, String dagName, String instanceName, DagNode<?, ?> node);

    /**
     * 当节点实例执行成功完成时调用。
     *
     * @param requestId     DAG 执行请求的唯一 ID。
     * @param dagName       正在执行的 DAG 定义的名称。
     * @param instanceName  成功完成的节点实例的名称。
     * @param totalDuration 节点从 onNodeStart 到成功的总持续时间。
     * @param logicDuration 节点核心逻辑执行的持续时间。
     * @param result        成功执行返回的 NodeResult (NodeResult<C, ?>)。
     * @param node          该实例使用的 DagNode 实现。
     */
    void onNodeSuccess(String requestId, String dagName, String instanceName, Duration totalDuration, Duration logicDuration, NodeResult<?, ?> result, DagNode<?, ?> node);

    /**
     * 当节点实例执行失败时调用。
     *
     * @param requestId     DAG 执行请求的唯一 ID。
     * @param dagName       正在执行的 DAG 定义的名称。
     * @param instanceName  失败的节点实例的名称。
     * @param totalDuration 节点从 onNodeStart 到失败的总持续时间。
     * @param logicDuration 节点核心逻辑执行的持续时间。
     * @param error         导致失败的错误。
     * @param node          该实例使用的 DagNode 实现。
     */
    void onNodeFailure(String requestId, String dagName, String instanceName, Duration totalDuration, Duration logicDuration, Throwable error, DagNode<?, ?> node);

    /**
     * 当节点实例执行因其 shouldExecute 条件为 false 或上游失败 (取决于策略) 而被跳过时调用。
     *
     * @param requestId DAG 执行请求的唯一 ID。
     * @param dagName   正在执行的 DAG 定义的名称。
     * @param instanceName 被跳过的节点实例的名称。
     * @param node      该实例使用的 DagNode 实现。
     */
    void onNodeSkipped(String requestId, String dagName, String instanceName, DagNode<?, ?> node);

    /**
     * 当节点实例执行尝试因超时而失败时调用。
     *
     * @param requestId DAG 执行请求的唯一 ID。
     * @param dagName   正在执行的 DAG 定义的名称。
     * @param instanceName 超时的节点实例的名称。
     * @param timeout   为节点配置的超时持续时间。
     * @param node      该实例使用的 DagNode 实现。
     */
    void onNodeTimeout(String requestId, String dagName, String instanceName, Duration timeout, DagNode<?, ?> node);

}
