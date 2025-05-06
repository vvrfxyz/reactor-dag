package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component("parallelNodeTypeA")
@Slf4j
public class ParallelNodeA implements DagNode<ParalleContext, String> {

    public static final InputSlot<String> INPUT_START_DATA = InputSlot.required("startData", String.class);
    public static final OutputSlot<String> OUTPUT_RESULT = OutputSlot.defaultOutput(String.class);

    @Override
    public Set<InputSlot<?>> getInputSlots() {
        return Set.of(INPUT_START_DATA);
    }

    @Override
    public OutputSlot<String> getOutputSlot() {
        return OUTPUT_RESULT;
    }

    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, InputAccessor<ParalleContext> inputs) {
        return Mono.defer(() -> {
            try {
                // 输入检查日志
                inputs.getPayload(INPUT_START_DATA)
                        .ifPresent(payload -> log.debug("{} logic received payload from input '{}': {}", this.getClass().getSimpleName(), INPUT_START_DATA.getId(), payload));
                if (!inputs.isAvailable(INPUT_START_DATA)) {
                    if (inputs.isFailed(INPUT_START_DATA)) log.warn("{} notes that input '{}' failed.", this.getClass().getSimpleName(), INPUT_START_DATA.getId());
                    else if (inputs.isSkipped(INPUT_START_DATA)) log.warn("{} notes that input '{}' was skipped.", this.getClass().getSimpleName(), INPUT_START_DATA.getId());
                    else if (inputs.isInactive(INPUT_START_DATA)) log.warn("{} notes that input '{}' was inactive.", this.getClass().getSimpleName(), INPUT_START_DATA.getId());
                    else log.warn("{} notes that input '{}' was not available.", this.getClass().getSimpleName(), INPUT_START_DATA.getId());
                    // 可以在这里决定是否因输入不足而失败或跳过
                    // return Mono.just(NodeResult.<ParalleContext, String>skipped());
                }

                String threadName = Thread.currentThread().getName();
                log.info("Executing {} logic on thread: {} (requires input '{}')", this.getClass().getSimpleName(), threadName, INPUT_START_DATA.getId());

                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted during sleep in {}", this.getClass().getSimpleName(), e);
                    return Mono.just(NodeResult.<ParalleContext, String>failure(e));
                }

                String resultPayload = this.getClass().getSimpleName() + " executed successfully on " + threadName;
                log.info("{} logic finished.", this.getClass().getSimpleName());

                Event<String> completionEvent = Event.of("NODE_COMPLETED", resultPayload);
                NodeResult<ParalleContext, String> successResult = NodeResult.success(resultPayload, Flux.just(completionEvent));

                return Mono.just(successResult);

            } catch (Throwable t) {
                log.error("Unexpected error executing {} logic: {}", this.getClass().getSimpleName(), t.getMessage(), t);
                return Mono.just(NodeResult.<ParalleContext, String>failure(t));
            }
        });
    }
}