// [文件名称]: DataParalleDag.java
package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import xyz.vvrf.reactor.dag.builder.ChainBuilder;
import xyz.vvrf.reactor.dag.core.NodeResult; // 引入 NodeResult，用于可能的自定义条件
import xyz.vvrf.reactor.dag.example.dataParalleDag.node.*;
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition;
// import java.time.Duration; // 如果使用 withTimeout
// import reactor.util.retry.Retry; // 如果使用 withRetry
import java.util.Map; // 引入 Map，用于可能的自定义条件

/**
 * reactor-dag
 * 定义一个扇出/扇入结构的 DAG，用于模拟并行执行。
 * 使用重构后的 ChainBuilder（支持条件边）和 NodeLogic。
 *
 * @author ruifeng.wen (重构)
 * @date (当前日期)
 */
@Component
@Slf4j
public class DataParalleDag extends AbstractDagDefinition<ParalleContext> {

    // DAG 的名称
    private static final String DAG_NAME = "DataParallelExampleDAG_ConditionalEdges"; // 名称稍作区分

    // 使用 NodeLogic 实现类的类名作为节点名
    private static final String FIRST_NODE_NAME = FirstNode.class.getSimpleName();
    private static final String PARALLEL_A_NAME = ParallelNodeA.class.getSimpleName();
    private static final String PARALLEL_B_NAME = ParallelNodeB.class.getSimpleName();
    private static final String PARALLEL_C_NAME = ParallelNodeC.class.getSimpleName();
    private static final String FINAL_NODE_NAME = FinalNode.class.getSimpleName();


    /**
     * 构造函数，通过 Spring 注入所有需要的 NodeLogic Bean。
     * 使用 ChainBuilder 定义 DAG 结构，采用新的依赖定义方式。
     */
    @Autowired
    public DataParalleDag(
            FirstNode firstNode,
            ParallelNodeA parallelNodeA,
            ParallelNodeB parallelNodeB,
            ParallelNodeC parallelNodeC,
            FinalNode finalNode
    ) {
        // 1. 调用父类构造函数
        super(ParalleContext.class, DAG_NAME);

        log.info("开始为 DAG '{}' (上下文: {}) 配置节点和依赖关系 (使用条件边)...",
                getDagName(), getContextType().getSimpleName());

        // 2. 创建 ChainBuilder 实例
        ChainBuilder<ParalleContext> builder = new ChainBuilder<>(this);

        try {
            // 3. 使用 ChainBuilder 定义节点实例及其依赖边
            //    采用 node().dependsOn().withCondition() 的链式调用

            builder
                    .node(FIRST_NODE_NAME, firstNode); // 起点节点，无依赖

            builder.node(PARALLEL_A_NAME, parallelNodeA)
                    .dependsOn(FIRST_NODE_NAME); // 依赖 FirstNode，使用默认条件 (FirstNode 成功)

            builder.node(PARALLEL_B_NAME, parallelNodeB)
                    .dependsOn(FIRST_NODE_NAME); // 依赖 FirstNode，使用默认条件

            builder.node(PARALLEL_C_NAME, parallelNodeC)
                    .dependsOn(FIRST_NODE_NAME); // 依赖 FirstNode，使用默认条件

            builder.node(FINAL_NODE_NAME, finalNode)
                    .dependsOn(PARALLEL_A_NAME) // 依赖 A，使用默认条件 (A 成功)
                    .dependsOn(PARALLEL_B_NAME) // 依赖 B，使用默认条件 (B 成功)
                    .dependsOn(PARALLEL_C_NAME); // 依赖 C，使用默认条件 (C 成功)
            // 默认情况下，FinalNode 会在 A, B, C 都成功后执行

            // 示例：为 FinalNode 的某条边添加自定义条件
            /*
            builder.node(FINAL_NODE_NAME, finalNode)
                   .dependsOn(PARALLEL_A_NAME) // 依赖 A
                   .withCondition((ctx, resultA) -> { // 自定义条件：A 必须成功
                       log.info("检查 FinalNode 对 A 的条件: A 是否成功? {}", resultA != null && resultA.isSuccess());
                       return resultA != null && resultA.isSuccess();
                   })
                   .dependsOn(PARALLEL_B_NAME) // 依赖 B
                   .withCondition((ctx, resultB) -> { // 自定义条件：B 成功或跳过都可以
                       log.info("检查 FinalNode 对 B 的条件: B 是否成功或跳过? {}", resultB != null && (resultB.isSuccess() || resultB.isSkipped()));
                       return resultB != null && (resultB.isSuccess() || resultB.isSkipped());
                   })
                   .dependsOn(PARALLEL_C_NAME); // 依赖 C，使用默认条件 (C 必须成功)
            */
            // 注意：即使 B 的条件允许跳过，FinalNode 仍然需要等待 B 的 Mono 完成才能评估条件。

            // 其他配置示例 (保持不变)
            // builder.withTimeout(PARALLEL_A_NAME, Duration.ofSeconds(1));
            // builder.withRetry(PARALLEL_B_NAME, Retry.backoff(3, Duration.ofMillis(100)));

            // 4. 将 Builder 中的配置应用到 DagDefinition
            builder.applyToDefinition();

            // 5. 初始化 DAG (验证、拓扑排序等)
            this.initialize();

            log.info("DAG '{}' 配置和初始化完成。", getDagName());

        } catch (IllegalStateException e) {
            log.error("配置或初始化 DAG '{}' 失败: {}", getDagName(), e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            log.error("配置或初始化 DAG '{}' 时发生意外错误: {}", getDagName(), e.getMessage(), e);
            throw new RuntimeException("初始化 DAG " + getDagName() + " 失败", e);
        }
    }

    /**
     * 实现 DagDefinition 接口要求的 getContextType 方法。
     */
    @Override
    public Class<ParalleContext> getContextType() {
        return ParalleContext.class;
    }
}
