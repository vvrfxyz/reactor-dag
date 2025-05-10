package xyz.vvrf.reactor.dag.monitor.web.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.NodeDefinition;
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;
import xyz.vvrf.reactor.dag.monitor.web.dto.MonitoringEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class RealtimeDagMonitorListener implements DagMonitorListener {

    // Key: dagName, Value: Sink for all events related to this DAG name
    private final Map<String, Sinks.Many<MonitoringEvent>> sinksByDagName = new ConcurrentHashMap<>();
    // Key: requestId, Value: Map<instanceName, MonitoringEvent> for current node states of a specific DAG run
    private final Map<String, Map<String, MonitoringEvent>> currentDagInstanceStates = new ConcurrentHashMap<>();
    // Key: requestId, Value: DagDefinition (to map requestId back to dagName if needed and for node details)
    private final Map<String, DagDefinition<?>> dagDefinitionsByRequestId = new ConcurrentHashMap<>();

    private static final long CLEANUP_DELAY_MINUTES = 30; // For requestId specific data

    // Special key for storing DAG_START event within currentDagInstanceStates
    private static final String DAG_EVENT_START_KEY = "_DAG_EVENT_DAG_START";
    // Special key for storing DAG_COMPLETE event
    private static final String DAG_EVENT_COMPLETE_KEY = "_DAG_EVENT_DAG_COMPLETE";


    public Flux<MonitoringEvent> getEventStream(String dagName) {
        Sinks.Many<MonitoringEvent> sink = sinksByDagName.computeIfAbsent(dagName, key ->
                Sinks.many().replay().<MonitoringEvent>limit(2000) // Replay a larger buffer for a DAG
        );

        // Flux for historical data of active/recent instances of this DAG
        Flux<MonitoringEvent> historicalStatesFlux = Flux.defer(() -> {
            List<MonitoringEvent> historicalEvents = new ArrayList<>();
            currentDagInstanceStates.forEach((reqId, instanceEventsMap) -> {
                DagDefinition<?> def = dagDefinitionsByRequestId.get(reqId);
                if (def != null && dagName.equals(def.getDagName())) {
                    // Add DAG_START event for this instance
                    MonitoringEvent dagStartEvent = instanceEventsMap.get(DAG_EVENT_START_KEY);
                    if (dagStartEvent != null) {
                        historicalEvents.add(dagStartEvent);
                    }

                    // Add all NODE_UPDATE events for this instance
                    instanceEventsMap.values().stream()
                            .filter(event -> event.getEventType() == MonitoringEvent.EventType.NODE_UPDATE)
                            .forEach(historicalEvents::add);

                    // Add DAG_COMPLETE event if it exists
                    MonitoringEvent dagCompleteEvent = instanceEventsMap.get(DAG_EVENT_COMPLETE_KEY);
                    if (dagCompleteEvent != null) {
                        historicalEvents.add(dagCompleteEvent);
                    }
                }
            });
            log.debug("Emitting {} historical events for DAG name '{}'", historicalEvents.size(), dagName);
            return Flux.fromIterable(historicalEvents);
        });

        return historicalStatesFlux.concatWith(sink.asFlux())
                .doOnSubscribe(subscription -> log.info("New subscriber for DAG name: {}", dagName))
                .doOnCancel(() -> log.info("Subscriber cancelled for DAG name: {}", dagName))
                .doOnError(e -> log.error("Error in event stream for DAG name: " + dagName, e));
    }

    private void emitEvent(MonitoringEvent event) {
        if (event.getDagName() == null) {
            log.warn("Cannot emit event, dagName is null: {}", event);
            return;
        }

        // Emit to the specific dagName sink
        Sinks.Many<MonitoringEvent> dagSink = sinksByDagName.get(event.getDagName());
        if (dagSink != null) {
            Sinks.EmitResult result = dagSink.tryEmitNext(event);
            if (result.isFailure() && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
                log.warn("Failed to emit monitoring event for dagName {}: {} (Event: {})", event.getDagName(), result, event);
            }
        } else {
            // This can happen if an event arrives before any client subscribes to this dagName.
            // The replay sink will handle this when a client eventually subscribes.
            // We still need to store the state.
            log.debug("No active sink for dagName {} when emitting event. Event will be replayed for new subscribers.", event.getDagName());
            // Ensure sink is created if it wasn't (e.g. event comes before first getEventStream call for this dagName)
            Sinks.Many<MonitoringEvent> newSink = sinksByDagName.computeIfAbsent(event.getDagName(), key ->
                    Sinks.many().replay().<MonitoringEvent>limit(2000)
            );
            newSink.tryEmitNext(event); // Emit to the newly created/retrieved sink
        }

        // Store/Update current state for the specific requestId
        Map<String, MonitoringEvent> instanceEvents = currentDagInstanceStates.computeIfAbsent(event.getRequestId(), k -> new ConcurrentHashMap<>());

        if (event.getEventType() == MonitoringEvent.EventType.NODE_UPDATE && event.getInstanceName() != null) {
            instanceEvents.put(event.getInstanceName(), event);
        } else if (event.getEventType() == MonitoringEvent.EventType.DAG_START) {
            instanceEvents.put(DAG_EVENT_START_KEY, event);
        } else if (event.getEventType() == MonitoringEvent.EventType.DAG_COMPLETE) {
            instanceEvents.put(DAG_EVENT_COMPLETE_KEY, event);
        }
        // No need for a generic "_DAG_EVENT_" + type key anymore with specific keys
    }

    private String summarizePayload(Object payload) {
        if (payload == null) return "null";
        String s = payload.toString();
        // Ensure payload class name is included for better context
        String className = payload.getClass().getSimpleName();
        if (s.length() > 150) return s.substring(0, 147) + "... (" + className + ")";
        return s + " (" + className + ")";
    }

    private String summarizeError(Throwable error) {
        if (error == null) return null;
        StringBuilder sb = new StringBuilder();
        sb.append(error.getClass().getSimpleName());
        if (error.getMessage() != null) {
            sb.append(": ").append(error.getMessage());
        }
        String summary = sb.toString();
        if (summary.length() > 200) return summary.substring(0, 197) + "...";
        return summary;
    }

    @Override
    public void onDagStart(String requestId, String dagName, DagDefinition<?> dagDefinition, Object initialContext) {
        log.debug("[Monitor] DAG Start: {} (Request ID: {}, DAG Name: {})", dagDefinition.getDagName(), requestId, dagName);
        dagDefinitionsByRequestId.put(requestId, dagDefinition);

        MonitoringEvent event = MonitoringEvent.builder()
                .requestId(requestId)
                .dagName(dagName) // Ensure dagName from definition is used if different from parameter
                .eventType(MonitoringEvent.EventType.DAG_START)
                .build();
        emitEvent(event);

        // Emit initial PENDING states for all nodes of this specific instance
        dagDefinition.getAllNodeInstanceNames().forEach(instanceName -> {
            NodeDefinition nodeDef = dagDefinition.getNodeDefinition(instanceName).orElse(null);
            String nodeTypeId = nodeDef != null ? nodeDef.getNodeTypeId() : "Unknown";
            MonitoringEvent nodeEvent = MonitoringEvent.builder()
                    .requestId(requestId)
                    .dagName(dagName)
                    .eventType(MonitoringEvent.EventType.NODE_UPDATE)
                    .instanceName(instanceName)
                    .nodeTypeId(nodeTypeId)
                    .nodeStatus(null) // Represents PENDING/INITIAL state before any execution attempt
                    .build();
            emitEvent(nodeEvent);
        });
    }

    @Override
    public void onDagComplete(String requestId, String dagName, DagDefinition<?> dagDefinition, Duration totalDuration, boolean success, Map<String, NodeResult<?, ?>> finalResults, Throwable error) {
        log.debug("[Monitor] DAG Complete: {} (Request ID: {}), Success: {}, Duration: {}", dagName, requestId, success, totalDuration);
        Map<String, NodeResult.NodeStatus> statuses = finalResults.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getStatus()));

        MonitoringEvent event = MonitoringEvent.builder()
                .requestId(requestId)
                .dagName(dagName)
                .eventType(MonitoringEvent.EventType.DAG_COMPLETE)
                .durationMillis(totalDuration.toMillis())
                .dagSuccess(success)
                .finalNodeStatuses(statuses) // This is useful for a summary, individual nodes would have their own final NODE_UPDATE
                .errorSummary(summarizeError(error))
                .build();
        emitEvent(event);

        // Schedule cleanup for this requestId's specific data
        Schedulers.boundedElastic().schedule(() -> {
            currentDagInstanceStates.remove(requestId);
            dagDefinitionsByRequestId.remove(requestId);
            log.debug("Cleaned up monitoring resources for completed requestId: {}", requestId);
        }, CLEANUP_DELAY_MINUTES, TimeUnit.MINUTES);
    }

    private void updateNodeState(String requestId, String dagName, String instanceName, NodeResult.NodeStatus status, Duration totalDuration, Duration logicDuration, Object payload, Throwable error) {
        DagDefinition<?> dagDef = dagDefinitionsByRequestId.get(requestId);
        String nodeTypeId = "Unknown";
        if (dagDef != null) {
            nodeTypeId = dagDef.getNodeDefinition(instanceName)
                    .map(NodeDefinition::getNodeTypeId)
                    .orElse("Unknown (" + instanceName + ")"); // More info if node not in def
        } else {
            log.warn("DagDefinition not found for requestId {} when updating node state for {}", requestId, instanceName);
        }

        MonitoringEvent event = MonitoringEvent.builder()
                .requestId(requestId)
                .dagName(dagName)
                .eventType(MonitoringEvent.EventType.NODE_UPDATE)
                .instanceName(instanceName)
                .nodeTypeId(nodeTypeId)
                .nodeStatus(status)
                .durationMillis(totalDuration != null ? totalDuration.toMillis() : null)
                .logicDurationMillis(logicDuration != null ? logicDuration.toMillis() : null)
                .payloadSummary(payload != null ? summarizePayload(payload) : null)
                .errorSummary(summarizeError(error))
                .build();
        emitEvent(event);
    }

    @Override
    public void onNodeStart(String requestId, String dagName, String instanceName, DagNode<?, ?> node) {
        // For onNodeStart, status is effectively RUNNING.
        // We use null for status in NODE_UPDATE to mean "RUNNING" or "actively being processed".
        // The initial PENDING state is sent during onDagStart.
        updateNodeState(requestId, dagName, instanceName, null, Duration.ZERO, Duration.ZERO, null, null);
    }

    @Override
    public void onNodeSuccess(String requestId, String dagName, String instanceName, Duration totalDuration, Duration logicDuration, NodeResult<?, ?> result, DagNode<?, ?> node) {
        updateNodeState(requestId, dagName, instanceName, NodeResult.NodeStatus.SUCCESS, totalDuration, logicDuration, result.getPayload().orElse(null), null);
    }

    @Override
    public void onNodeFailure(String requestId, String dagName, String instanceName, Duration totalDuration, Duration logicDuration, Throwable error, DagNode<?, ?> node) {
        updateNodeState(requestId, dagName, instanceName, NodeResult.NodeStatus.FAILURE, totalDuration, logicDuration, null, error);
    }

    @Override
    public void onNodeSkipped(String requestId, String dagName, String instanceName, DagNode<?, ?> node) {
        updateNodeState(requestId, dagName, instanceName, NodeResult.NodeStatus.SKIPPED, null, null, null, null);
    }

    @Override
    public void onNodeTimeout(String requestId, String dagName, String instanceName, Duration timeout, DagNode<?, ?> node) {
        log.warn("[Monitor] Node Timeout: {} in DAG {} (Request ID: {}) after {} ms", instanceName, dagName, requestId, timeout.toMillis());
        // Typically, a timeout will lead to onNodeFailure. If not, and a specific TIMEOUT status is desired:
        // updateNodeState(requestId, dagName, instanceName, NodeResult.NodeStatus.TIMEOUT, timeout, timeout, null, new TimeoutException("Node timed out after " + timeout.toMillis() + "ms"));
        // For now, relying on onNodeFailure to report the terminal state.
    }
}
