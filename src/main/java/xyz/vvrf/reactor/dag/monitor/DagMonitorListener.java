// file: monitor/DagMonitorListener.java (基本保持不变，参数类型可能微调)
package xyz.vvrf.reactor.dag.monitor;

import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.time.Duration;

/**
 * 用于监控 DAG 节点执行事件的监听器接口。
 *
 * @author Refactored
 */
public interface DagMonitorListener {

    void onNodeStart(String requestId, String dagName, String instanceName, DagNode<?, ?> node);

    void onNodeSuccess(String requestId, String dagName, String instanceName, Duration totalDuration, Duration logicDuration, NodeResult<?, ?> result, DagNode<?, ?> node);

    void onNodeFailure(String requestId, String dagName, String instanceName, Duration totalDuration, Duration logicDuration, Throwable error, DagNode<?, ?> node);

    void onNodeSkipped(String requestId, String dagName, String instanceName, DagNode<?, ?> node);

    void onNodeTimeout(String requestId, String dagName, String instanceName, Duration timeout, DagNode<?, ?> node);

}
