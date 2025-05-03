// file: example/dataParalleDag/DataParalleDagConfiguration.java (Renamed and changed)
package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean; // Use Spring context for factory
import org.springframework.context.annotation.Configuration; // Use Spring context for factory
import xyz.vvrf.reactor.dag.builder.DagDefinitionBuilder;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.example.dataParalleDag.node.*; // Import node classes

/**
 * 使用 DagDefinitionBuilder 配置并行 DAG。
 * 作为 Spring Configuration 类提供 DagDefinition Bean。
 */
@Configuration // Mark as Spring configuration
@Slf4j
public class DataParalleDagConfiguration {

    // Define unique type IDs for node implementations
    private static final String TYPE_FIRST = FirstNode.class.getName();
    private static final String TYPE_PARALLEL_A = ParallelNodeA.class.getName();
    private static final String TYPE_PARALLEL_B = ParallelNodeB.class.getName();
    private static final String TYPE_PARALLEL_C = ParallelNodeC.class.getName();
    private static final String TYPE_FINAL = FinalNode.class.getName();

    // Define instance names used in the graph
    private static final String INSTANCE_START = "startNode";
    private static final String INSTANCE_PROC_A = "parallelProcessorA";
    private static final String INSTANCE_PROC_B = "parallelProcessorB";
    private static final String INSTANCE_PROC_C = "parallelProcessorC";
    private static final String INSTANCE_AGGREGATOR = "finalAggregator";

    @Bean // Expose the DagDefinition as a Spring Bean
    public DagDefinition<ParalleContext> dataParallelDagDefinition() {
        log.info("开始构建 DataParallel DAG 定义...");

        // 1. 创建构建器
        DagDefinitionBuilder<ParalleContext> builder = new DagDefinitionBuilder<>(
                ParalleContext.class, // Context 类型
                "DataParallelDAG"     // DAG 名称
        ); // Can set errorStrategy here if needed: .errorStrategy(...)

        // 2. 注册节点实现 (逻辑节点)
        builder.registerImplementation(TYPE_FIRST, new FirstNode());
        builder.registerImplementation(TYPE_PARALLEL_A, new ParallelNodeA());
        builder.registerImplementation(TYPE_PARALLEL_B, new ParallelNodeB());
        builder.registerImplementation(TYPE_PARALLEL_C, new ParallelNodeC());
        builder.registerImplementation(TYPE_FINAL, new FinalNode());
        log.info("已注册所有节点实现。");

        // 3. 添加节点实例到图中
        builder.addNode(INSTANCE_START, TYPE_FIRST);
        builder.addNode(INSTANCE_PROC_A, TYPE_PARALLEL_A);
        builder.addNode(INSTANCE_PROC_B, TYPE_PARALLEL_B);
        builder.addNode(INSTANCE_PROC_C, TYPE_PARALLEL_C);
        builder.addNode(INSTANCE_AGGREGATOR, TYPE_FINAL);
        log.info("已添加所有节点实例。");

        // 4. 定义连接关系 (Wiring)
        // Parallel nodes depend on the start node's output ("startData" input slot)
        builder.wire(INSTANCE_PROC_A, "startData", INSTANCE_START); // procA.startData <- startNode.output
        builder.wire(INSTANCE_PROC_B, "startData", INSTANCE_START); // procB.startData <- startNode.output
        builder.wire(INSTANCE_PROC_C, "startData", INSTANCE_START); // procC.startData <- startNode.output

        // Final node depends on the parallel nodes' outputs
        builder.wire(INSTANCE_AGGREGATOR, "resultA", INSTANCE_PROC_A); // aggregator.resultA <- procA.output
        builder.wire(INSTANCE_AGGREGATOR, "resultB", INSTANCE_PROC_B); // aggregator.resultB <- procB.output
        builder.wire(INSTANCE_AGGREGATOR, "resultC", INSTANCE_PROC_C); // aggregator.resultC <- procC.output
        log.info("已定义所有节点连接。");

        // 5. 构建并返回不可变的 DagDefinition
        try {
            DagDefinition<ParalleContext> definition = builder.build();
            log.info("DataParallel DAG 定义构建成功！");
            return definition;
        } catch (IllegalStateException e) {
            log.error("构建 DataParallel DAG 定义失败: {}", e.getMessage(), e);
            // 在实际应用中，可能需要更健壮的错误处理，例如阻止应用启动
            throw new RuntimeException("Failed to build DataParallel DAG definition", e);
        }
    }
}
