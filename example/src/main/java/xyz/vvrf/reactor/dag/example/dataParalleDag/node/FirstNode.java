package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import lombok.extern.slf4j.Slf4j;
// 移除 org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.annotation.DagNodeType; // 引入新注解
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@DagNodeType(id = "firstNodeType", contextType = ParalleContext.class) // 指定 ID 和上下文类型
@Slf4j
public class FirstNode implements DagNode<ParalleContext, String> {

    public static final OutputSlot<String> OUTPUT_DATA = OutputSlot.defaultOutput(String.class);

    @Override
    public Set<InputSlot<?>> getInputSlots() {
        return Collections.emptySet();
    }

    @Override
    public OutputSlot<String> getOutputSlot() {
        return OUTPUT_DATA;
    }

    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, InputAccessor<ParalleContext> inputs) {
        return Mono.defer(() -> {
            try {
                String threadName = Thread.currentThread().getName();
                log.info("Executing {} logic on thread: {}", this.getClass().getSimpleName(), threadName);

                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted during sleep in {}", this.getClass().getSimpleName(), e);
                    return Mono.just(NodeResult.<ParalleContext, String>failure(e));
                }

                String resultPayload = this.getClass().getSimpleName() + " executed successfully on " + threadName;
                log.info("{} logic finished.", this.getClass().getSimpleName());

                Event<Object> completionEvent = Event.builder()
                        .event("NODE_COMPLETED")
                        .data(resultPayload)
                        .comment(this.getClass().getSimpleName() + " finished execution.")
                        .build();
                NodeResult<ParalleContext, String> successResult = NodeResult.success(resultPayload, Flux.just(completionEvent));

                return Mono.just(successResult);

            } catch (Throwable t) {
                log.error("Unexpected error executing {} logic: {}", this.getClass().getSimpleName(), t.getMessage(), t);
                return Mono.just(NodeResult.<ParalleContext, String>failure(t));
            }
        });
    }
}
