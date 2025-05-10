package xyz.vvrf.reactor.dag.monitor.web.dto;

import lombok.Builder;
import lombok.Data;
import xyz.vvrf.reactor.dag.core.NodeResult; // 确保可以访问

import java.util.Map;

@Data
@Builder
public class MonitoringEvent {
    private String requestId;
    private String dagName;
    private EventType eventType; // DAG_START, NODE_UPDATE, DAG_COMPLETE

    // Node specific
    private String instanceName;
    private String nodeTypeId;
    private NodeResult.NodeStatus nodeStatus; // 使用核心库的枚举
    private String payloadSummary;
    private String errorSummary;
    private Long durationMillis;
    private Long logicDurationMillis;

    // DAG Complete specific
    private Boolean dagSuccess;
    private Map<String, NodeResult.NodeStatus> finalNodeStatuses;

    public enum EventType {
        DAG_START, NODE_UPDATE, DAG_COMPLETE
    }
}
