package xyz.vvrf.reactor.dag.monitor;

/**
 * reactor-dag
 *
 * @author ruifeng.wen
 * @date 2025/4/25
 */

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class MicrometerDagMonitorListener implements DagMonitorListener {

    private final MeterRegistry meterRegistry;

    // 指标名称
    private static final String METRIC_NODE_EXECUTION_TIME = "dag.node.execution.time";
    private static final String METRIC_NODE_EXECUTION_TOTAL = "dag.node.execution.total";

    // 标签键
    private static final String TAG_DAG_NAME = "dag.name";
    private static final String TAG_NODE_NAME = "node.name";
    private static final String TAG_STATUS = "status";
    private static final String TAG_ERROR = "error";

    // 状态标签值
    private static final String STATUS_SUCCESS = "SUCCESS";
    private static final String STATUS_FAILURE = "FAILURE";
    private static final String STATUS_SKIPPED = "SKIPPED";
    private static final String STATUS_TIMEOUT = "TIMEOUT";


    @Autowired
    public MicrometerDagMonitorListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void onNodeStart(String requestId, String dagName, String nodeName, DagNode<?, ?, ?> node) {
        // 通常计数器/计时器在结束时记录，但如果需要，可以在此处设置一个 "尝试" 计数器。
    }

    @Override
    public void onNodeSuccess(String requestId, String dagName, String nodeName, Duration duration, NodeResult<?, ?, ?> result) {
        Tags tags = Tags.of(
                Tag.of(TAG_DAG_NAME, dagName),
                Tag.of(TAG_NODE_NAME, nodeName),
                Tag.of(TAG_STATUS, STATUS_SUCCESS)
        );
        recordTimer(tags, duration);
        incrementCounter(tags);
    }

    @Override
    public void onNodeFailure(String requestId, String dagName, String nodeName, Duration duration, Throwable error, DagNode<?, ?, ?> node) {
        String errorTagValue = error != null ? error.getClass().getSimpleName() : "Unknown";
        // 显式检查 TimeoutException
        String status = (error instanceof TimeoutException) ? STATUS_TIMEOUT : STATUS_FAILURE;

        Tags tags = Tags.of(
                Tag.of(TAG_DAG_NAME, dagName),
                Tag.of(TAG_NODE_NAME, nodeName),
                Tag.of(TAG_STATUS, status),
                Tag.of(TAG_ERROR, errorTagValue)
        );
        recordTimer(tags, duration);
        incrementCounter(tags);
    }

    @Override
    public void onNodeSkipped(String requestId, String dagName, String nodeName, DagNode<?, ?, ?> node) {
        Tags tags = Tags.of(
                Tag.of(TAG_DAG_NAME, dagName),
                Tag.of(TAG_NODE_NAME, nodeName),
                Tag.of(TAG_STATUS, STATUS_SKIPPED)
        );
        incrementCounter(tags);
    }

    @Override
    public void onNodeTimeout(String requestId, String dagName, String nodeName, Duration timeout, DagNode<?, ?, ?> node) {
        // 超时通常是失败的 *原因*。
        // onNodeFailure 方法已经特别标记了 TimeoutException。
        // 如果需要，此方法可以增加一个特定的超时计数器，与最终的失败计数分开。
        Tags tags = Tags.of(
                Tag.of(TAG_DAG_NAME, dagName),
                Tag.of(TAG_NODE_NAME, nodeName),
                Tag.of(TAG_STATUS, STATUS_TIMEOUT)
        );
        Counter.builder("dag.node.timeout.total").tags(tags).register(meterRegistry).increment();
        log.debug("Micrometer 监听器捕获到节点 {} 的超时事件", nodeName);
    }

    private void recordTimer(Tags tags, Duration duration) {
        try {
            Timer timer = Timer.builder(METRIC_NODE_EXECUTION_TIME)
                    .tags(tags)
                    .description("DAG 节点执行时间")
                    .register(meterRegistry);
            timer.record(duration.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Exception e) {
            log.error("记录计时器指标失败: {}", e.getMessage(), e);
        }
    }

    private void incrementCounter(Tags tags) {
        try {
            Counter counter = Counter.builder(METRIC_NODE_EXECUTION_TOTAL)
                    .tags(tags)
                    .description("按状态统计的 DAG 节点执行总数")
                    .register(meterRegistry);
            counter.increment();
        } catch (Exception e) {
            log.error("增加计数器指标失败: {}", e.getMessage(), e);
        }
    }
}