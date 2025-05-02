// [文件名称]: FirstNode.java
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
 * DAG 的起始节点。
 * 实现 NodeLogic<ParalleContext, String>，产生 String 类型的事件。
 * execute 方法签名已更新，不再接收 DependencyAccessor。
 *
 * @author ruifeng.wen (重构)
 * @date (当前日期)
 */
@Component
@Slf4j
public class FirstNode implements NodeLogic<ParalleContext, String> {

    @Override
    public Class<String> getEventType() {
        return String.class;
    }

    /**
     * 执行起始节点逻辑。
     *
     * @param context 并行上下文。
     * @return 包含节点执行结果（成功时包含一个事件）的 Mono。
     */
    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context) { // 签名变更
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            log.info("Executing {} on thread: {}", this.getClass().getSimpleName(), threadName);

            // 模拟一些工作
            try {
                TimeUnit.MILLISECONDS.sleep(50); // 少量延迟
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Error during sleep in {}: {}", this.getClass().getSimpleName(), e.getMessage());
                // 返回失败结果
                return NodeResult.failure(context, e, getEventType());
            }

            String resultData = this.getClass().getSimpleName() + " executed successfully on " + threadName;
            log.info("{} finished.", this.getClass().getSimpleName());

            // 如果需要将结果传递给下游，在这里写入 Context
            // context.getNodeResults().put(this.getClass().getSimpleName(), "SUCCESS"); // 示例

            // 创建成功事件 (用于最终输出)
            Event<String> successEvent = Event.of(
                    this.getClass().getSimpleName() + "Success", // 事件类型
                    resultData // 事件数据
            );

            // 返回成功结果，包含事件流
            return NodeResult.success(context, Flux.just(successEvent), getEventType());

        }).onErrorResume(error -> {
            // 捕获 Callable 内部未处理的异常
            log.error("Unexpected error executing {}: {}", this.getClass().getSimpleName(), error.getMessage(), error);
            // 可以在这里向 Context 写入失败状态
            // context.getNodeResults().put(this.getClass().getSimpleName(), "FAILURE: " + error.getMessage());
            return Mono.just(NodeResult.failure(context, error, getEventType()));
        });
    }
}
