// [file name]: DagMonitorListener.java
package xyz.vvrf.reactor.dag.monitor;

import xyz.vvrf.reactor.dag.core.DagNodeDefinition; // 使用 Definition
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.time.Duration;

/**
 * 用于监控 DAG 节点执行事件的监听器接口。
 * 实现类可用于发送指标、记录详细执行信息等。
 */
public interface DagMonitorListener {

    /**
     * 当一个节点即将被执行时调用（在依赖解析之后，shouldExecute 检查之前）。
     *
     * @param requestId    DAG 执行请求的唯一 ID。
     * @param dagName      正在执行的 DAG 定义的名称。
     * @param nodeName     即将开始的节点的名称。
     * @param nodeDefinition 正在执行的 DagNodeDefinition 实例。
     */
    void onNodeStart(String requestId, String dagName, String nodeName, DagNodeDefinition<?, ?> nodeDefinition);

    /**
     * 当节点执行成功完成时调用。
     *
     * @param requestId     DAG 执行请求的唯一 ID。
     * @param dagName       正在执行的 DAG 定义的名称。
     * @param nodeName      成功完成的节点的名称。
     * @param totalDuration 节点从 onNodeStart 到成功的总持续时间。
     * @param logicDuration 节点核心逻辑执行的持续时间 (从 execute 调用开始计时)。
     * @param result        成功执行返回的 NodeResult。
     * @param nodeDefinition DagNodeDefinition 实例。
     */
    void onNodeSuccess(String requestId, String dagName, String nodeName, Duration totalDuration, Duration logicDuration, NodeResult<?, ?> result, DagNodeDefinition<?, ?> nodeDefinition);

    /**
     * 当节点执行失败时调用（如果适用，在重试之后）。
     *
     * @param requestId     DAG 执行请求的唯一 ID。
     * @param dagName       正在执行的 DAG 定义的名称。
     * @param nodeName      失败的节点的名称。
     * @param totalDuration 节点从 onNodeStart 到失败的总持续时间。
     * @param logicDuration 节点核心逻辑执行的持续时间 (可能为 0 或部分执行时间)。
     * @param error         导致失败的错误。
     * @param nodeDefinition 失败的 DagNodeDefinition 实例。
     */
    void onNodeFailure(String requestId, String dagName, String nodeName, Duration totalDuration, Duration logicDuration, Throwable error, DagNodeDefinition<?, ?> nodeDefinition);

    /**
     * 当节点执行因其 shouldExecute 条件为 false 而被跳过时调用。
     *
     * @param requestId DAG 执行请求的唯一 ID。
     * @param dagName   正在执行的 DAG 定义的名称。
     * @param nodeName  被跳过的节点的名称。
     * @param nodeDefinition 被跳过的 DagNodeDefinition 实例。
     */
    void onNodeSkipped(String requestId, String dagName, String nodeName, DagNodeDefinition<?, ?> nodeDefinition);

    /**
     * 当节点执行尝试因超时而失败时调用。
     * 这通常在 onNodeFailure 之前或同时发生（超时是失败的原因）。
     *
     * @param requestId DAG 执行请求的唯一 ID。
     * @param dagName   正在执行的 DAG 定义的名称。
     * @param nodeName  超时的节点的名称。
     * @param timeout   为节点配置的超时持续时间。
     * @param nodeDefinition 超时的 DagNodeDefinition 实例。
     */
    void onNodeTimeout(String requestId, String dagName, String nodeName, Duration timeout, DagNodeDefinition<?, ?> nodeDefinition);

}
