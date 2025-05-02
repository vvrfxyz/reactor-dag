// [文件名称]: ParallelNodeB.java
package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
// import xyz.vvrf.reactor.dag.core.DependencyAccessor; // 移除导入
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.core.NodeLogic;
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.concurrent.TimeUnit;

/**
 * reactor-dag
 * 另一个并行执行的节点，依赖于 FirstNode。
 * 实现 NodeLogic<ParalleContext, String>。
 * 不再使用 DependencyAccessor，执行结果写入 Context。
 *
 * @author (你的名字) (重构)
 * @date (当前日期)
 */
@Component
@Slf4j
public class ParallelNodeB implements NodeLogic<ParalleContext, String> {

    // private static final String DEPENDENCY_NAME = "FirstNode";

    @Override
    public Class<String> getEventType() {
        return String.class;
    }

    /**
     * 执行并行节点 B 的逻辑。
     * 将执行结果写入共享的 Context 中。
     *
     * @param context 并行上下文。
     * @return 包含节点执行结果的 Mono。
     */
    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context) { // 签名变更
        return Mono.fromCallable(() -> {
            String nodeName = this.getClass().getSimpleName();
            String threadName = Thread.currentThread().getName();
            log.info("Executing {} on thread: {}", nodeName, threadName);

            String resultData;
            String statusMessage = "SUCCESS";

            // 模拟耗时工作
            try {
                TimeUnit.MILLISECONDS.sleep(500); // 较长延迟
                resultData = nodeName + " executed successfully on " + threadName;
                log.info("{} finished.", nodeName);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Error during sleep in {}: {}", nodeName, e.getMessage());
                statusMessage = "FAILURE: Interrupted";
                context.getNodeResults().put(nodeName, statusMessage);
                return NodeResult.failure(context, e, getEventType());
            } catch (Exception e) {
                log.error("Unexpected error during execution in {}: {}", nodeName, e.getMessage(), e);
                statusMessage = "FAILURE: " + e.getClass().getSimpleName();
                context.getNodeResults().put(nodeName, statusMessage);
                return NodeResult.failure(context, e, getEventType());
            }

            // 将成功结果写入 Context
            context.getNodeResults().put(nodeName, statusMessage);

            // 创建成功事件
            Event<String> successEvent = Event.of(nodeName + "Success", resultData);

            // 返回成功结果
            return NodeResult.success(context, Flux.just(successEvent), getEventType());

        }).onErrorResume(error -> {
            String nodeName = this.getClass().getSimpleName();
            log.error("Unexpected error in Mono chain for {}: {}", nodeName, error.getMessage(), error);
            context.getNodeResults().put(nodeName, "FAILURE: Async Error");
            return Mono.just(NodeResult.failure(context, error, getEventType()));
        });
    }
}
