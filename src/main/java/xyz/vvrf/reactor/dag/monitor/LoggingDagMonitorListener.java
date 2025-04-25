package xyz.vvrf.reactor.dag.monitor;

/**
 * reactor-dag
 *
 * @author ruifeng.wen
 * @date 2025/4/25
 */

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.time.Duration;

@Slf4j
@Component // 注册为 Spring 组件以便自动装配
public class LoggingDagMonitorListener implements DagMonitorListener {

    @Override
    public void onNodeStart(String requestId, String dagName, String nodeName, DagNode<?, ?, ?> node) {
        log.info("[MONITOR] 请求:[{}] DAG:[{}] 节点:[{}] 开始。 类:[{}]",
                requestId, dagName, nodeName, node.getClass().getSimpleName());
    }

    @Override
    public void onNodeSuccess(String requestId, String dagName, String nodeName, Duration duration, NodeResult<?, ?, ?> result) {
        log.info("[MONITOR] 请求:[{}] DAG:[{}] 节点:[{}] 成功。 耗时:[{}ms], Payload存在:[{}], 事件存在:[{}]",
                requestId, dagName, nodeName, duration.toMillis(), result.getPayload().isPresent(), result.getEvents() != null);
    }

    @Override
    public void onNodeFailure(String requestId, String dagName, String nodeName, Duration duration, Throwable error, DagNode<?, ?, ?> node) {
        log.error("[MONITOR] 请求:[{}] DAG:[{}] 节点:[{}] 失败。 耗时:[{}ms], 错误:[{}], 类:[{}]",
                requestId, dagName, nodeName, duration.toMillis(), error.getMessage(), node.getClass().getSimpleName(), error);
    }

    @Override
    public void onNodeSkipped(String requestId, String dagName, String nodeName, DagNode<?, ?, ?> node) {
        log.info("[MONITOR] 请求:[{}] DAG:[{}] 节点:[{}] 跳过。 类:[{}]",
                requestId, dagName, nodeName, node.getClass().getSimpleName());
    }

    @Override
    public void onNodeTimeout(String requestId, String dagName, String nodeName, Duration timeout, DagNode<?, ?, ?> node) {
        log.warn("[MONITOR] 请求:[{}] DAG:[{}] 节点:[{}] 超时。 配置:[{}ms], 类:[{}]",
                requestId, dagName, nodeName, timeout.toMillis(), node.getClass().getSimpleName());
    }
}