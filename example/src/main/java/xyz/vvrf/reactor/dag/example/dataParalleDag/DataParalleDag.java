// [文件名称]: DataParalleDag.java
package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import xyz.vvrf.reactor.dag.builder.ChainBuilder;
import xyz.vvrf.reactor.dag.example.dataParalleDag.node.*;
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition;
// import xyz.vvrf.reactor.dag.core.NodeResult; // 如果使用 when，可能需要导入
// import java.time.Duration; // 如果使用 withTimeout
// import reactor.util.retry.Retry; // 如果使用 withRetry

/**
 * reactor-dag
 * 定义一个扇出/扇入结构的 DAG，用于模拟并行执行。
 * 使用重构后的 ChainBuilder 和 NodeLogic。
 *
 * @author ruifeng.wen (重构)
 * @date (当前日期)
 */
@Component
@Slf4j
public class DataParalleDag extends AbstractDagDefinition<ParalleContext> {

    // DAG 的名称
    private static final String DAG_NAME = "DataParallelExampleDAG";

    // 使用 NodeLogic 实现类的类名作为节点名，更清晰且不易出错
    private static final String FIRST_NODE_NAME = FirstNode.class.getSimpleName();
    private static final String PARALLEL_A_NAME = ParallelNodeA.class.getSimpleName();
    private static final String PARALLEL_B_NAME = ParallelNodeB.class.getSimpleName();
    private static final String PARALLEL_C_NAME = ParallelNodeC.class.getSimpleName();
    private static final String FINAL_NODE_NAME = FinalNode.class.getSimpleName();


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
            //    使用定义的常量作为节点名
            builder
                    .node(FIRST_NODE_NAME, firstNode) // 起点节点

                    .node(PARALLEL_A_NAME, parallelNodeA, FIRST_NODE_NAME) // 并行节点 A
                    .node(PARALLEL_B_NAME, parallelNodeB, FIRST_NODE_NAME) // 并行节点 B
                    .node(PARALLEL_C_NAME, parallelNodeC, FIRST_NODE_NAME) // 并行节点 C

                    .node(FINAL_NODE_NAME, finalNode, PARALLEL_A_NAME, PARALLEL_B_NAME, PARALLEL_C_NAME); // 最终节点

            // 示例：为特定节点添加配置 (保持不变)
            // builder.withTimeout(PARALLEL_A_NAME, Duration.ofSeconds(1));
            // builder.withRetry(PARALLEL_B_NAME, Retry.backoff(3, Duration.ofMillis(100)));

            // 示例：使用新的 when 签名 (如果需要)
            // builder.when(FINAL_NODE_NAME, (ctx, depResults) ->
            //     depResults.containsKey(PARALLEL_A_NAME) && depResults.get(PARALLEL_A_NAME).isSuccess()
            // ); // 仅当 A 成功时执行 FinalNode

            // 4. 将 Builder 中的配置应用到 DagDefinition
            builder.applyToDefinition();

            // 5. 初始化 DAG (验证、拓扑排序等)
            this.initialize(); // 这个方法来自 AbstractDagDefinition

            log.info("DAG '{}' 配置和初始化完成。", getDagName());

        } catch (IllegalStateException e) {
            // 捕获构建或初始化过程中的错误
            log.error("配置或初始化 DAG '{}' 失败: {}", getDagName(), e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            // 捕获其他意外异常
            log.error("配置或初始化 DAG '{}' 时发生意外错误: {}", getDagName(), e.getMessage(), e);
            throw new RuntimeException("初始化 DAG " + getDagName() + " 失败", e);
        }
    }

    /**
     * 实现 DagDefinition 接口要求的 getContextType 方法。
     * 返回此 DAG 定义关联的具体上下文类型。
     *
     * @return 上下文类型的 Class 对象。
     */
    @Override
    public Class<ParalleContext> getContextType() {
        // 直接返回在父类构造函数中指定的具体类型
        return ParalleContext.class;
    }

}
