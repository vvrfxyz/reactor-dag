package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component("firstNodeType")
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
        // 使用 Mono.defer 来包裹同步逻辑，并在内部处理异常
        return Mono.defer(() -> {
            try {
                String threadName = Thread.currentThread().getName();
                log.info("Executing {} logic on thread: {}", this.getClass().getSimpleName(), threadName);

                // 模拟耗时操作
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted during sleep in {}", this.getClass().getSimpleName(), e);
                    // 直接返回失败结果
                    return Mono.just(NodeResult.<ParalleContext, String>failure(e));
                }

                // 成功逻辑
                String resultPayload = this.getClass().getSimpleName() + " executed successfully on " + threadName;
                log.info("{} logic finished.", this.getClass().getSimpleName());

                Event<Object> completionEvent = Event.builder()
                        .event("NODE_COMPLETED")
                        .data(resultPayload)
                        .comment(this.getClass().getSimpleName() + " finished execution.")
                        .build();
                NodeResult<ParalleContext, String> successResult = NodeResult.success(resultPayload, Flux.just(completionEvent));

                // 直接返回成功结果
                return Mono.just(successResult);

            } catch (Throwable t) { // 捕获所有其他未预料的异常
                log.error("Unexpected error executing {} logic: {}", this.getClass().getSimpleName(), t.getMessage(), t);
                // 直接返回失败结果
                return Mono.just(NodeResult.<ParalleContext, String>failure(t));
            }
        }); // 结束 Mono.defer
        // 不再需要 onErrorResume
    }
}