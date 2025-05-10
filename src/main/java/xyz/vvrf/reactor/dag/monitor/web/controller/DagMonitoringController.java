package xyz.vvrf.reactor.dag.monitor.web.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.monitor.web.dto.MonitoringEvent;
import xyz.vvrf.reactor.dag.monitor.web.service.DagDefinitionCache;
import xyz.vvrf.reactor.dag.monitor.web.service.RealtimeDagMonitorListener;

import java.util.UUID;

@RestController
@RequestMapping("/dag-monitor") // Base path
public class DagMonitoringController {

    private final RealtimeDagMonitorListener monitorListenerService;
    private final DagDefinitionCache dagDefinitionCache;
    private final ObjectMapper objectMapper;

    public DagMonitoringController(RealtimeDagMonitorListener monitorListenerService,
                                   DagDefinitionCache dagDefinitionCache,
                                   ObjectMapper objectMapper) {
        this.monitorListenerService = monitorListenerService;
        this.dagDefinitionCache = dagDefinitionCache;
        this.objectMapper = objectMapper;
    }

    // Changed {requestId} to {dagName}
    @GetMapping(value = "/stream/{dagName}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> streamEventsByDagName(@PathVariable String dagName) {
        return monitorListenerService.getEventStream(dagName)
                .map(monitoringEvent -> {
                    try {
                        // Ensure event type in SSE is from the MonitoringEvent object
                        String eventTypeString = monitoringEvent.getEventType() != null ?
                                monitoringEvent.getEventType().name() : "UNKNOWN_EVENT";
                        return ServerSentEvent.<String>builder()
                                .id(UUID.randomUUID().toString()) // Unique ID for each SSE event
                                .event(eventTypeString) // DAG_START, NODE_UPDATE, DAG_COMPLETE
                                .data(objectMapper.writeValueAsString(monitoringEvent))
                                .build();
                    } catch (JsonProcessingException e) {
                        // Log error and possibly return an error type SSE
                        return ServerSentEvent.<String>builder()
                                .id(UUID.randomUUID().toString())
                                .event("SERIALIZATION_ERROR")
                                .data("{\"error\":\"Failed to serialize monitoring event: " + e.getMessage() + "\", \"dagName\":\"" + dagName + "\"}")
                                .build();
                    }
                });
    }

    @GetMapping("/dag-definition/{dagName}/dot")
    public Mono<String> getDagDefinitionDot(@PathVariable String dagName) {
        return dagDefinitionCache.getDotRepresentation(dagName)
                // If dot representation is not found, return 404 with a clear message
                .map(Mono::just)
                .orElseGet(() -> Mono.error(new DagDefinitionNotFoundException("DAG Definition DOT not found for: " + dagName + ". Ensure it was cached after building.")));
    }

    // Custom exception for clearer 404s
    @ResponseStatus(value = org.springframework.http.HttpStatus.NOT_FOUND)
    public static class DagDefinitionNotFoundException extends RuntimeException {
        public DagDefinitionNotFoundException(String message) {
            super(message);
        }
    }
}
