package xyz.vvrf.reactor.dag.monitor;

import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.time.Duration;

/**
 * 用于监控 DAG 节点执行事件的监听器接口。
 * 实现类可用于发送指标、记录详细执行信息等。
 *
 * 注意：为简单起见，此初始版本不包含上下文对象 'C'。
 * 如果特定监听器需要访问它，可以稍后添加。
 */
public interface DagMonitorListener {

    /**
     * 当一个节点即将被执行时调用（在依赖解析之后，shouldExecute 检查之前）。
     *
     * @param requestId DAG 执行请求的唯一 ID。
     * @param dagName   正在执行的 DAG 定义的名称。
     * @param nodeName  即将开始的节点的名称。
     * @param node      正在执行的 DagNode 实例。
     */
    void onNodeStart(String requestId, String dagName, String nodeName, DagNode<?, ?, ?> node);

    /**
     * 当节点执行成功完成时调用。
     *
     * @param requestId     DAG 执行请求的唯一 ID。
     * @param dagName       正在执行的 DAG 定义的名称。
     * @param nodeName      成功完成的节点的名称。
     * @param totalDuration 节点从 onNodeStart 到成功的总持续时间。
     * @param logicDuration 节点核心逻辑执行的持续时间 (从 execute 调用开始计时)。
     * @param result        成功执行返回的 NodeResult。
     * @param node          DagNode 实例。
     */
    void onNodeSuccess(String requestId, String dagName, String nodeName, Duration totalDuration, Duration logicDuration, NodeResult<?, ?, ?> result, DagNode<?, ?, ?> node);

    /**
     * 当节点执行失败时调用（如果适用，在重试之后）。
     *
     * @param requestId     DAG 执行请求的唯一 ID。
     * @param dagName       正在执行的 DAG 定义的名称。
     * @param nodeName      失败的节点的名称。
     * @param totalDuration 节点从 onNodeStart 到失败的总持续时间。
     * @param logicDuration 节点核心逻辑执行的持续时间 (从 execute 调用开始计时)。 // 新增
     * @param error         导致失败的错误。
     * @param node          失败的 DagNode 实例。
     */
    void onNodeFailure(String requestId, String dagName, String nodeName, Duration totalDuration, Duration logicDuration, Throwable error, DagNode<?, ?, ?> node);

    /**
     * 当节点执行因其 shouldExecute 条件为 false 而被跳过时调用。
     *
     * @param requestId DAG 执行请求的唯一 ID。
     * @param dagName   正在执行的 DAG 定义的名称。
     * @param nodeName  被跳过的节点的名称。
     * @param node      被跳过的 DagNode 实例。
     */
    void onNodeSkipped(String requestId, String dagName, String nodeName, DagNode<?, ?, ?> node);

    /**
     * 当节点执行尝试因超时而失败时调用。
     * 如果超时是重试后的最终错误，这可能在 onNodeFailure 之前调用。
     *
     * @param requestId DAG 执行请求的唯一 ID。
     * @param dagName   正在执行的 DAG 定义的名称。
     * @param nodeName  超时的节点的名称。
     * @param timeout   为节点配置的超时持续时间。
     * @param node      超时的 DagNode 实例。
     */
    void onNodeTimeout(String requestId, String dagName, String nodeName, Duration timeout, DagNode<?, ?, ?> node);

    // 可选：如果需要整体 DAG 监控，可以添加 onDagStart, onDagEnd
    // void onDagStart(String requestId, String dagName, C context, DagDefinition<C> dagDefinition);
    // void onDagEnd(String requestId, String dagName, Duration duration, Status status); // Status 可以是 SUCCESS, FAILED
}