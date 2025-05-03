// file: example/dataParalleDag/node/ParallelNodeC.java
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

/**
 * 并行节点 C 的逻辑。
 * 使用 SLF4j 日志并产生一个完成事件。
 */
@Slf4j // Add Slf4j logging
public class ParallelNodeC implements DagNode<ParalleContext, String> {

    private static final String INPUT_SLOT_NAME = "startData";

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    @Override
    public Map<String, Class<?>> getInputRequirements() {
        return Map.of(INPUT_SLOT_NAME, String.class);
    }

    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, InputAccessor<ParalleContext> inputs) {
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            // Use logger
            log.info("Executing {} logic on thread: {} (requires input '{}')", this.getClass().getSimpleName(), threadName, INPUT_SLOT_NAME);

            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted during sleep in {}", this.getClass().getSimpleName(), e);
                return NodeResult.failure(context, e, String.class);
            }

            String resultPayload = this.getClass().getSimpleName() + " executed successfully on " + threadName;
            log.info("{} logic finished.", this.getClass().getSimpleName());

            // Create an event
            Event<String> completionEvent = Event.of("NODE_COMPLETED", resultPayload);

            // Return success with payload and event flux
            return NodeResult.success(context, resultPayload, Flux.just(completionEvent), String.class); // Pass event flux

        }).onErrorResume(error -> {
            // Use logger
            log.error("Error executing {} logic: {}", this.getClass().getSimpleName(), error.getMessage(), error);
            return Mono.just(NodeResult.failure(context, error, String.class));
        });
    }
}
