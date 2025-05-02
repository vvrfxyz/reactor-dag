// [文件名称]: FinalNode.java
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

import java.util.Arrays;
import java.util.List;
import java.util.Map; // 引入 Map
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * reactor-dag
 * 最终节点，依赖于所有并行节点，作为同步点。
 * 实现 NodeLogic<ParalleContext, String>，产生包含聚合状态的 String 事件。
 * 从 Context 中读取依赖节点写入的结果/状态。
 *
 * @author (你的名字) (重构)
 * @date (当前日期)
 */
@Component
@Slf4j
public class FinalNode implements NodeLogic<ParalleContext, String> {

    // 显式定义期望的依赖节点名称 (用于从 Context 中查找结果)
    private static final List<String> EXPECTED_DEPENDENCIES = Arrays.asList(
            ParallelNodeA.class.getSimpleName(), // 使用类名更健壮
            ParallelNodeB.class.getSimpleName(),
            ParallelNodeC.class.getSimpleName()
    );

    @Override
    public Class<String> getEventType() {
        return String.class; // 最终产生一个聚合结果字符串事件
    }

    /**
     * 执行最终节点逻辑，聚合来自并行节点的状态 (从 Context 读取)。
     *
     * @param context 并行上下文。
     * @return 包含最终聚合结果的 Mono。
     */
    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context) { // 签名变更
        return Mono.fromCallable(() -> {
            String nodeName = this.getClass().getSimpleName();
            String threadName = Thread.currentThread().getName();
            log.info("Executing {} on thread: {}", nodeName, threadName);

            // 从 Context 中聚合依赖节点的状态
            Map<String, String> dependencyResults = context.getNodeResults();
            String aggregatedStatus = EXPECTED_DEPENDENCIES.stream()
                    .map(depName -> {
                        // 从 context 获取结果，如果不存在则标记为 NOT_FOUND
                        String status = dependencyResults.getOrDefault(depName, "NOT_FOUND");
                        return depName + ": [" + status + "]";
                    })
                    .collect(Collectors.joining("; "));

            log.info("{} received aggregated dependency status from context: {}", nodeName, aggregatedStatus);

            // 模拟最终处理
            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Error during sleep in {}: {}", nodeName, e.getMessage());
                return NodeResult.failure(context, e, getEventType());
            }

            String resultData = String.format("%s finished successfully on %s. Aggregated Status: %s",
                    nodeName, threadName, aggregatedStatus);
            log.info("{} finished.", nodeName);

            // 创建成功事件 (用于最终输出)
            Event<String> finalEvent = Event.of(nodeName + "Complete", resultData);

            // 返回成功结果
            return NodeResult.success(context, Flux.just(finalEvent), getEventType());

        }).onErrorResume(error -> {
            log.error("Unexpected error executing {}: {}", this.getClass().getSimpleName(), error.getMessage(), error);
            return Mono.just(NodeResult.failure(context, error, getEventType()));
        });
    }

    // 移除 shouldExecute 示例，因为默认行为（所有依赖成功）通常是 FinalNode 所需的
    // 如果需要自定义逻辑（例如，即使部分失败也要执行），可以覆盖 shouldExecute:
    /*
    @Override
    public boolean shouldExecute(ParalleContext context, Map<String, NodeResult<ParalleContext, ?>> dependencyResults) {
        // 示例：只要至少有一个并行节点成功就执行
        boolean anySuccess = EXPECTED_DEPENDENCIES.stream()
                               .anyMatch(name -> dependencyResults.containsKey(name) && dependencyResults.get(name).isSuccess());
        log.info("FinalNode shouldExecute check: Any parallel node succeeded? {}", anySuccess);
        return anySuccess;
        // 或者总是执行： return true;
    }
    */
}
