// file: example/dataParalleDag/node/FinalNode.java
package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import lombok.extern.slf4j.Slf4j; // Import Slf4j
import reactor.core.publisher.Flux; // Import Flux
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.Event; // Import Event
import xyz.vvrf.reactor.dag.core.InputAccessor;
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 最终节点逻辑，聚合结果。
 * 使用 SLF4j 日志并产生一个包含聚合结果的事件。
 */
@Slf4j // Add Slf4j logging
public class FinalNode implements DagNode<ParalleContext, String> {

    private static final String INPUT_A = "resultA";
    private static final String INPUT_B = "resultB";
    private static final String INPUT_C = "resultC";

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    @Override
    public Map<String, Class<?>> getInputRequirements() {
        return Map.of(
                INPUT_A, String.class,
                INPUT_B, String.class,
                INPUT_C, String.class
        );
    }

    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, InputAccessor<ParalleContext> inputs) {
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            // Use logger
            log.info("Executing {} logic on thread: {}", this.getClass().getSimpleName(), threadName);

            // Aggregate results
            String aggregatedPayloads = getInputRequirements().keySet().stream()
                    .map(inputSlotName -> {
                        String payloadStr = inputs.getPayload(inputSlotName, String.class)
                                .orElseGet(() -> {
                                    if (inputs.isInputFailed(inputSlotName)) {
                                        log.warn("{} detected FAILED input for slot '{}'", this.getClass().getSimpleName(), inputSlotName);
                                        return "FAILED";
                                    }
                                    if (inputs.isInputSkipped(inputSlotName)) {
                                        log.warn("{} detected SKIPPED input for slot '{}'", this.getClass().getSimpleName(), inputSlotName);
                                        return "SKIPPED";
                                    }
                                    log.warn("{} detected EMPTY or unavailable input for slot '{}'", this.getClass().getSimpleName(), inputSlotName);
                                    return "EMPTY";
                                });
                        return inputSlotName + ": [" + payloadStr + "]";
                    })
                    .collect(Collectors.joining("; "));

            // Use logger (debug for detailed aggregation result)
            log.debug("{} logic received aggregated results: {}", this.getClass().getSimpleName(), aggregatedPayloads);

            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted during sleep in {}", this.getClass().getSimpleName(), e);
                return NodeResult.failure(context, e, String.class);
            }

            String resultPayload = this.getClass().getSimpleName() + " finished successfully on " + threadName + ". Aggregated: " + aggregatedPayloads;
            log.info("{} logic finished.", this.getClass().getSimpleName());

            // Create an event with the aggregated data
            Event<Object> aggregationEvent = Event.builder()
                    .event("FINAL_AGGREGATION")
                    .data(aggregatedPayloads) // Event data is the aggregated string
                    .comment("Final aggregation completed.")
                    .build();

            // Return success with payload and event flux
            return NodeResult.success(context, resultPayload, Flux.just(aggregationEvent), String.class); // Pass event flux

        }).onErrorResume(error -> {
            // Use logger
            log.error("Error executing {} logic: {}", this.getClass().getSimpleName(), error.getMessage(), error);
            return Mono.just(NodeResult.failure(context, error, String.class));
        });
    }
}
