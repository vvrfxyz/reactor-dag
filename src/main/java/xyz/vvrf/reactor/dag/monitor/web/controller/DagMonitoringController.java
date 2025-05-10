package xyz.vvrf.reactor.dag.monitor.web.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity; // 用于返回 JSON
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

    @GetMapping(value = "/stream/{dagName}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> streamEventsByDagName(@PathVariable String dagName) {
        return monitorListenerService.getEventStream(dagName)
                .map(monitoringEvent -> {
                    try {
                        String eventTypeString = monitoringEvent.getEventType() != null ?
                                monitoringEvent.getEventType().name() : "UNKNOWN_EVENT";
                        return ServerSentEvent.<String>builder()
                                .id(UUID.randomUUID().toString())
                                .event(eventTypeString)
                                .data(objectMapper.writeValueAsString(monitoringEvent))
                                .build();
                    } catch (JsonProcessingException e) {
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
                .map(Mono::just)
                .orElseGet(() -> Mono.error(new DagDefinitionNotFoundException("DAG Definition DOT not found for: " + dagName + ". Ensure it was cached after building.")));
    }

    // 修改端点：获取 Cytoscape.js JSON
    @GetMapping(value = "/dag-definition/{dagName}/cytoscapejs", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<String>> getDagDefinitionCytoscapeJs(@PathVariable String dagName) {
        return dagDefinitionCache.getCytoscapeJsRepresentation(dagName)
                .map(jsonString -> Mono.just(ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(jsonString)))
                .orElseGet(() -> Mono.just(ResponseEntity.notFound().build()));
    }


    @ResponseStatus(value = org.springframework.http.HttpStatus.NOT_FOUND)
    public static class DagDefinitionNotFoundException extends RuntimeException {
        public DagDefinitionNotFoundException(String message) {
            super(message);
        }
    }
}
