package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DependencyAccessor;
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.core.NodeLogic;
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.concurrent.TimeUnit;

/**
 * reactor-dag
 * 一个并行执行的节点，依赖于 FirstNode。
 * 实现 NodeLogic<ParalleContext, String>，产生 String 类型的事件。
 * 使用 DependencyAccessor 访问依赖结果。
 *
 * @author (你的名字) (重构)
 * @date (当前日期)
 */
@Component
@Slf4j
public class ParallelNodeA implements NodeLogic<ParalleContext, String> {

    private static final String DEPENDENCY_NAME = "FirstNode"; // 显式定义依赖名称

    @Override
    public Class<String> getEventType() {
        return String.class;
    }

    /**
     * 执行并行节点 A 的逻辑。
     *
     * @param context      并行上下文。
     * @param dependencies 依赖访问器。
     * @return 包含节点执行结果的 Mono。
     */
    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, DependencyAccessor<ParalleContext> dependencies) {

        // 示例：检查依赖节点的状态
        if (dependencies.isSuccess(DEPENDENCY_NAME)) {
            log.info("{} notes that dependency '{}' succeeded.", this.getClass().getSimpleName(), DEPENDENCY_NAME);
            // 示例：尝试获取依赖节点的事件 (注意：事件流可能为空或包含多个事件)
            dependencies.getEvents(DEPENDENCY_NAME)
                    .next() // 只取第一个事件示例
                    .subscribe(event -> log.info("{} received event data from {}: {}",
                            this.getClass().getSimpleName(), DEPENDENCY_NAME, event.getData()));
        } else {
            log.warn("{} notes that dependency '{}' did not succeed (Status: {}).",
                    this.getClass().getSimpleName(), DEPENDENCY_NAME,
                    dependencies.getResult(DEPENDENCY_NAME).map(r -> r.getStatus().name()).orElse("NOT_FOUND"));
            // 可以根据依赖失败情况决定是否继续执行或直接返回失败/跳过
            // 此处示例继续执行
        }

        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            log.info("Executing {} on thread: {} (depends on {})", this.getClass().getSimpleName(), threadName, DEPENDENCY_NAME);

            // 模拟耗时工作
            try {
                TimeUnit.MILLISECONDS.sleep(500); // 较长延迟
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Error during sleep in {}: {}", this.getClass().getSimpleName(), e.getMessage());
                return NodeResult.failure(context, e, getEventType());
            }

            String resultData = this.getClass().getSimpleName() + " executed successfully on " + threadName;
            log.info("{} finished.", this.getClass().getSimpleName());

            // 创建成功事件
            Event<String> successEvent = Event.of(this.getClass().getSimpleName() + "Success", resultData);

            // 返回成功结果
            return NodeResult.success(context, Flux.just(successEvent), getEventType());

        }).onErrorResume(error -> {
            log.error("Unexpected error executing {}: {}", this.getClass().getSimpleName(), error.getMessage(), error);
            return Mono.just(NodeResult.failure(context, error, getEventType()));
        });
    }
}
