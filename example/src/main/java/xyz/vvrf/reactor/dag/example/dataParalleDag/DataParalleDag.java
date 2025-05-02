package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import xyz.vvrf.reactor.dag.builder.ChainBuilder;
import xyz.vvrf.reactor.dag.example.dataParalleDag.node.*; // 引入所有 NodeLogic 实现
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition;

/**
 * reactor-dag
 * 定义一个扇出/扇入结构的 DAG，用于模拟并行执行。
 * 使用新的 ChainBuilder 和 NodeLogic。
 *
 * @author ruifeng.wen (重构)
 * @date (当前日期)
 */
@Component
@Slf4j
public class DataParalleDag extends AbstractDagDefinition<ParalleContext> {

    // DAG 的名称
    private static final String DAG_NAME = "DataParallelExampleDAG";

    /**
     * 构造函数，通过 Spring 注入所有需要的 NodeLogic Bean。
     * 使用 ChainBuilder 定义 DAG 结构。
     *
     * @param firstNode     起始节点逻辑
     * @param parallelNodeA 并行节点 A 逻辑
     * @param parallelNodeB 并行节点 B 逻辑
     * @param parallelNodeC 并行节点 C 逻辑
     * @param finalNode     最终节点逻辑
     */
    @Autowired
    public DataParalleDag(
            FirstNode firstNode,
            ParallelNodeA parallelNodeA,
            ParallelNodeB parallelNodeB,
            ParallelNodeC parallelNodeC,
            FinalNode finalNode
    ) {
        // 1. 调用父类构造函数，传入上下文类型和 DAG 名称
        super(ParalleContext.class, DAG_NAME);

        log.info("开始为 DAG '{}' (上下文: {}) 配置节点和依赖关系...",
                getDagName(), getContextType().getSimpleName());

        // 2. 创建 ChainBuilder 实例
        ChainBuilder<ParalleContext> builder = new ChainBuilder<>(this);

        try {
            // 3. 使用 ChainBuilder 定义节点实例及其依赖
            //    - 第一个参数是节点在 DAG 中的唯一名称 (String)
            //    - 第二个参数是该节点使用的 NodeLogic Bean
            //    - 后续参数是该节点依赖的其他节点的名称 (String...)
            builder
                    .node("FirstNode", firstNode) // 起点节点，无依赖

                    .node("ParallelA", parallelNodeA, "FirstNode") // 并行节点 A，依赖 FirstNode
                    .node("ParallelB", parallelNodeB, "FirstNode") // 并行节点 B，依赖 FirstNode
                    .node("ParallelC", parallelNodeC, "FirstNode") // 并行节点 C，依赖 FirstNode

                    .node("FinalNode", finalNode, "ParallelA", "ParallelB", "ParallelC"); // 最终节点，依赖所有并行节点

            // 可以为特定节点添加配置，例如超时或重试 (示例)
            // builder.withTimeout("ParallelA", Duration.ofSeconds(1));
            // builder.withRetry("ParallelB", Retry.backoff(3, Duration.ofMillis(100)));
            // builder.when("FinalNode", (ctx, deps) -> deps.isSuccess("ParallelA")); // 仅当 A 成功时执行 FinalNode

            // 4. 将 Builder 中的配置应用到 DagDefinition
            builder.applyToDefinition();

            // 5. 初始化 DAG (验证、拓扑排序等)
            this.initialize(); // 这个方法来自 AbstractDagDefinition

            log.info("DAG '{}' 配置和初始化完成。", getDagName());

        } catch (IllegalStateException e) {
            // 捕获构建或初始化过程中的错误 (例如，依赖缺失、循环依赖、节点重名等)
            log.error("配置或初始化 DAG '{}' 失败: {}", getDagName(), e.getMessage(), e);
            // 根据需要处理异常，例如阻止应用启动
            throw e;
        } catch (Exception e) {
            // 捕获其他意外异常
            log.error("配置或初始化 DAG '{}' 时发生意外错误: {}", getDagName(), e.getMessage(), e);
            throw new RuntimeException("Failed to initialize DAG " + getDagName(), e);
        }
    }

    @Override
    public Class<ParalleContext> getContextType() {
        return ParalleContext.class;
    }
}
