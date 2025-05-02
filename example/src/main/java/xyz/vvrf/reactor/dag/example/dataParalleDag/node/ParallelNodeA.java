// [文件名称]: ParallelNodeA.java
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
 * 一个并行执行的节点，依赖于 FirstNode。
 * 实现 NodeLogic<ParalleContext, String>，产生 String 类型的事件。
 * 不再使用 DependencyAccessor，执行结果写入 Context。
 *
 * @author (你的名字) (重构)
 * @date (当前日期)
 */
@Component
@Slf4j
public class ParallelNodeA implements NodeLogic<ParalleContext, String> {

    // 依赖名称主要用于 DAG 定义，执行时不再直接访问依赖状态
    // private static final String DEPENDENCY_NAME = "FirstNode";

    @Override
    public Class<String> getEventType() {
        return String.class;
    }

    /**
     * 执行并行节点 A 的逻辑。
     * 假设执行条件（如 FirstNode 成功）由框架或 shouldExecute 处理。
     * 将执行结果写入共享的 Context 中。
     *
     * @param context 并行上下文。
     * @return 包含节点执行结果的 Mono。
     */
    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context) { // 签名变更

        // 移除所有 DependencyAccessor 的使用
        // 依赖检查逻辑（如果需要）应移至 shouldExecute 或假设框架已处理

        return Mono.fromCallable(() -> {
            String nodeName = this.getClass().getSimpleName();
            String threadName = Thread.currentThread().getName();
            log.info("Executing {} on thread: {}", nodeName, threadName);

            String resultData;
            String statusMessage = "SUCCESS"; // 初始状态

            // 模拟耗时工作
            try {
                TimeUnit.MILLISECONDS.sleep(500); // 较长延迟
                resultData = nodeName + " executed successfully on " + threadName;
                log.info("{} finished.", nodeName);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Error during sleep in {}: {}", nodeName, e.getMessage());
                statusMessage = "FAILURE: Interrupted";
                // 将失败状态写入 Context
                context.getNodeResults().put(nodeName, statusMessage);
                return NodeResult.failure(context, e, getEventType());
            } catch (Exception e) { // 捕获其他可能的运行时异常
                log.error("Unexpected error during execution in {}: {}", nodeName, e.getMessage(), e);
                statusMessage = "FAILURE: " + e.getClass().getSimpleName();
                // 将失败状态写入 Context
                context.getNodeResults().put(nodeName, statusMessage);
                return NodeResult.failure(context, e, getEventType());
            }

            // 将成功结果写入 Context
            context.getNodeResults().put(nodeName, statusMessage); // 或者写入 resultData

            // 创建成功事件 (用于最终输出)
            Event<String> successEvent = Event.of(nodeName + "Success", resultData);

            // 返回成功结果
            return NodeResult.success(context, Flux.just(successEvent), getEventType());

        }).onErrorResume(error -> {
            // 这个 onErrorResume 主要捕获 Callable 之外的错误 (例如 Mono 操作链中的错误)
            String nodeName = this.getClass().getSimpleName();
            log.error("Unexpected error in Mono chain for {}: {}", nodeName, error.getMessage(), error);
            // 尝试将最终失败状态写入 Context
            context.getNodeResults().put(nodeName, "FAILURE: Async Error");
            return Mono.just(NodeResult.failure(context, error, getEventType()));
        });
    }
}
