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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * reactor-dag
 * 最终节点，依赖于所有并行节点，作为同步点。
 * 实现 NodeLogic<ParalleContext, String>，产生包含聚合状态的 String 事件。
 * 使用 DependencyAccessor 检查依赖状态。
 *
 * @author (你的名字) (重构)
 * @date (当前日期)
 */
@Component
@Slf4j
public class FinalNode implements NodeLogic<ParalleContext, String> {

    // 显式定义期望的依赖节点名称
    private static final List<String> EXPECTED_DEPENDENCIES = Arrays.asList("ParallelA", "ParallelB", "ParallelC");

    @Override
    public Class<String> getEventType() {
        return String.class; // 最终产生一个聚合结果字符串事件
    }

    /**
     * 执行最终节点逻辑，聚合来自并行节点的状态。
     *
     * @param context      并行上下文。
     * @param dependencies 依赖访问器。
     * @return 包含最终聚合结果的 Mono。
     */
    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, DependencyAccessor<ParalleContext> dependencies) {
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            log.info("Executing {} on thread: {}", this.getClass().getSimpleName(), threadName);

            // 使用 DependencyAccessor 聚合依赖节点的状态
            String aggregatedStatus = EXPECTED_DEPENDENCIES.stream()
                    .map(depName -> {
                        String status;
                        if (dependencies.isSuccess(depName)) {
                            status = "SUCCESS";
                        } else if (dependencies.isFailure(depName)) {
                            status = "FAILURE";
                        } else if (dependencies.isSkipped(depName)) {
                            status = "SKIPPED";
                        } else if (dependencies.contains(depName)) {
                            // 存在但状态未知？理论上不太可能，除非 NodeResult 扩展了状态
                            status = dependencies.getResult(depName).map(r -> r.getStatus().name()).orElse("UNKNOWN_STATE");
                        } else {
                            status = "NOT_FOUND"; // 依赖不存在？初始化时应已捕获
                        }
                        return depName + ": [" + status + "]";
                    })
                    .collect(Collectors.joining("; "));

            log.info("{} received aggregated dependency status: {}", this.getClass().getSimpleName(), aggregatedStatus);

            // 模拟最终处理
            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Error during sleep in {}: {}", this.getClass().getSimpleName(), e.getMessage());
                return NodeResult.failure(context, e, getEventType());
            }

            String resultData = String.format("%s finished successfully on %s. Aggregated Status: %s",
                    this.getClass().getSimpleName(), threadName, aggregatedStatus);
            log.info("{} finished.", this.getClass().getSimpleName());

            // 创建成功事件
            Event<String> finalEvent = Event.of(this.getClass().getSimpleName() + "Complete", resultData);

            // 返回成功结果
            return NodeResult.success(context, Flux.just(finalEvent), getEventType());

        }).onErrorResume(error -> {
            log.error("Unexpected error executing {}: {}", this.getClass().getSimpleName(), error.getMessage(), error);
            return Mono.just(NodeResult.failure(context, error, getEventType()));
        });
    }
}
