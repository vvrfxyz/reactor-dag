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
 * 另一个并行执行的节点，依赖于 FirstNode。
 * 实现 NodeLogic<ParalleContext, String>。
 * (此示例中未使用 DependencyAccessor 访问依赖数据，但签名保持一致)
 *
 * @author (你的名字) (重构)
 * @date (当前日期)
 */
@Component
@Slf4j
public class ParallelNodeB implements NodeLogic<ParalleContext, String> {

    private static final String DEPENDENCY_NAME = "FirstNode";

    @Override
    public Class<String> getEventType() {
        return String.class;
    }

    /**
     * 执行并行节点 B 的逻辑。
     *
     * @param context      并行上下文。
     * @param dependencies 依赖访问器 (未使用)。
     * @return 包含节点执行结果的 Mono。
     */
    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, DependencyAccessor<ParalleContext> dependencies) {
        // dependencies 参数未使用，但签名需匹配接口
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
