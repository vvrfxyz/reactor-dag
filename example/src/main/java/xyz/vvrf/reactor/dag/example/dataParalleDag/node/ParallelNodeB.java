package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import lombok.extern.slf4j.Slf4j;
// 移除 org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.annotation.DagNodeType; // 引入新注解
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.Collections; // 引入 Collections
import java.util.HashSet; // 引入 HashSet
import java.util.Set;
import java.util.concurrent.TimeUnit;

// 使用 @DagNodeType 注解
@DagNodeType(id = "parallelNodeTypeB", contextType = ParalleContext.class) // 指定 ID 和上下文类型
@Slf4j
public class ParallelNodeB implements DagNode<ParalleContext, String> {

    public static final InputSlot<String> INPUT_START_DATA = InputSlot.required("startData", String.class);
    public static final OutputSlot<String> OUTPUT_RESULT = OutputSlot.defaultOutput(String.class);

    @Override
    public Set<InputSlot<?>> getInputSlots() {
        // JDK 8
        return new HashSet<>(Collections.singletonList(INPUT_START_DATA));
    }

    @Override
    public OutputSlot<String> getOutputSlot() {
        return OUTPUT_RESULT;
    }

    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, InputAccessor<ParalleContext> inputs) {
        return Mono.defer(() -> {
            try {
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
