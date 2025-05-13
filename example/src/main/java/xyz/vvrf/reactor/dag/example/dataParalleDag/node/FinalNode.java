package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import lombok.extern.slf4j.Slf4j;
// 移除 org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.annotation.DagNodeType; // 引入新注解
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

// 使用 @DagNodeType 注解
@DagNodeType(id = "finalNodeType", contextType = ParalleContext.class) // 指定 ID 和上下文类型
@Slf4j
public class FinalNode implements DagNode<ParalleContext, String> {

    public static final InputSlot<String> INPUT_A = InputSlot.required("resultA", String.class);
    public static final InputSlot<String> INPUT_B = InputSlot.required("resultB", String.class);
    public static final InputSlot<String> INPUT_C = InputSlot.required("resultC", String.class);
    public static final OutputSlot<String> OUTPUT_AGGREGATED = OutputSlot.defaultOutput(String.class);

    @Override
    public Set<InputSlot<?>> getInputSlots() {
        return new HashSet<>(Arrays.asList(INPUT_A, INPUT_B, INPUT_C));
    }

    @Override
    public OutputSlot<String> getOutputSlot() {
        return OUTPUT_AGGREGATED;
    }

    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, InputAccessor<ParalleContext> inputs) {
        return Mono.defer(() -> {
            try {
                String threadName = Thread.currentThread().getName();
                log.info("Executing {} logic on thread: {}", this.getClass().getSimpleName(), threadName);

                // JDK 8 使用 HashSet 构造输入槽集合
                Set<InputSlot<?>> inputSlotsSet = new HashSet<>(Arrays.asList(INPUT_A, INPUT_B, INPUT_C));

                String aggregatedPayloads = inputSlotsSet.stream()
                        .map(inputSlot -> {
                            @SuppressWarnings("unchecked")
                            InputSlot<String> typedSlot = (InputSlot<String>) inputSlot;
                            String payloadStr = inputs.getPayload(typedSlot)
                                    .orElseGet(() -> {
                                        if (inputs.isFailed(typedSlot)) return "FAILED";
                                        if (inputs.isSkipped(typedSlot)) return "SKIPPED";
                                        if (inputs.isInactive(typedSlot)) return "INACTIVE";
                                        return "EMPTY";
                                    });
                            return typedSlot.getId() + ": [" + payloadStr + "]";
                        })
                        .collect(Collectors.joining("; "));

                log.debug("{} logic received aggregated results: {}", this.getClass().getSimpleName(), aggregatedPayloads);

                try {
                    TimeUnit.MILLISECONDS.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted during sleep in {}", this.getClass().getSimpleName(), e);
                    return Mono.just(NodeResult.<ParalleContext, String>failure(e));
                }

                String resultPayload = this.getClass().getSimpleName() + " finished successfully on " + threadName + ". Aggregated: " + aggregatedPayloads;
                log.info("{} logic finished.", this.getClass().getSimpleName());

                Event<Object> aggregationEvent = Event.builder()
                        .event("FINAL_AGGREGATION")
                        .data(aggregatedPayloads)
                        .comment("Final aggregation completed.")
                        .build();
                NodeResult<ParalleContext, String> successResult = NodeResult.success(resultPayload, Flux.just(aggregationEvent));

                return Mono.just(successResult);

            } catch (Throwable t) {
                log.error("Unexpected error executing {} logic: {}", this.getClass().getSimpleName(), t.getMessage(), t);
                return Mono.just(NodeResult.<ParalleContext, String>failure(t));
            }
        });
    }
}
