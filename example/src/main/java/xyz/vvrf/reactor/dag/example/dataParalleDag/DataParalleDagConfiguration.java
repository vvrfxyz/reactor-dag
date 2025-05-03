// file: example/dataParalleDag/DataParalleDagConfiguration.java (Refactored)
package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired; // 用于构造函数注入
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import xyz.vvrf.reactor.dag.builder.DagDefinitionBuilder;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.DagNode; // 引入 DagNode
import xyz.vvrf.reactor.dag.example.dataParalleDag.node.*; // 引入节点类

import java.util.Objects;

/**
 * 使用 DagDefinitionBuilder 配置并行 DAG (重构版)。
 * 作为 Spring Configuration 类提供 DagDefinition Bean。
 * 利用依赖注入管理节点实例，并直接添加节点实现。
 */
@Configuration // 标记为 Spring 配置类
@Slf4j
public class DataParalleDagConfiguration {

    // 定义图中使用的节点实例名称 (保持不变)
    private static final String INSTANCE_START = "startNode";
    private static final String INSTANCE_PROC_A = "parallelProcessorA";
    private static final String INSTANCE_PROC_B = "parallelProcessorB";
    private static final String INSTANCE_PROC_C = "parallelProcessorC";
    private static final String INSTANCE_AGGREGATOR = "finalAggregator";

    // --- 依赖注入节点实例 ---
    private final FirstNode firstNode;
    private final ParallelNodeA parallelNodeA;
    private final ParallelNodeB parallelNodeB;
    private final ParallelNodeC parallelNodeC;
    private final FinalNode finalNode;

    /**
     * 通过构造函数注入所有需要的 DagNode Bean。
     * Spring 会自动查找并注入这些类型的 Bean。
     *
     * @param firstNode      FirstNode 的 Bean 实例
     * @param parallelNodeA  ParallelNodeA 的 Bean 实例
     * @param parallelNodeB  ParallelNodeB 的 Bean 实例
     * @param parallelNodeC  ParallelNodeC 的 Bean 实例
     * @param finalNode      FinalNode 的 Bean 实例
     */
    @Autowired // 明确标注构造函数用于注入
    public DataParalleDagConfiguration(
            FirstNode firstNode,
            ParallelNodeA parallelNodeA,
            ParallelNodeB parallelNodeB,
            ParallelNodeC parallelNodeC,
            FinalNode finalNode) {
        this.firstNode = Objects.requireNonNull(firstNode, "FirstNode bean cannot be null");
        this.parallelNodeA = Objects.requireNonNull(parallelNodeA, "ParallelNodeA bean cannot be null");
        this.parallelNodeB = Objects.requireNonNull(parallelNodeB, "ParallelNodeB bean cannot be null");
        this.parallelNodeC = Objects.requireNonNull(parallelNodeC, "ParallelNodeC bean cannot be null");
        this.finalNode = Objects.requireNonNull(finalNode, "FinalNode bean cannot be null");
        log.info("DataParalleDagConfiguration: 所有节点 Bean 已成功注入。");
    }

    @Bean // 将构建好的 DagDefinition 暴露为 Spring Bean
    public DagDefinition<ParalleContext> dataParallelDagDefinition() {
        log.info("开始构建 DataParallel DAG 定义 (使用注入的节点)...");

        // 1. 创建构建器
        DagDefinitionBuilder<ParalleContext> builder = new DagDefinitionBuilder<>(
                ParalleContext.class, // Context 类型
                "DataParallelDAG"     // DAG 名称
        );

        // 2. 添加节点实例到图中 (直接使用注入的 Bean)
        // 不再需要 registerImplementation
        log.info("添加节点实例...");
        builder.addNode(INSTANCE_START, this.firstNode); // 直接使用注入的 firstNode Bean
        builder.addNode(INSTANCE_PROC_A, this.parallelNodeA); // 使用注入的 parallelNodeA Bean
        builder.addNode(INSTANCE_PROC_B, this.parallelNodeB); // 使用注入的 parallelNodeB Bean
        builder.addNode(INSTANCE_PROC_C, this.parallelNodeC); // 使用注入的 parallelNodeC Bean
        builder.addNode(INSTANCE_AGGREGATOR, this.finalNode); // 使用注入的 finalNode Bean
        log.info("已添加所有节点实例。");

        // 3. 定义连接关系 (Wiring) - 逻辑保持不变，依赖实例名称
        log.info("定义节点连接...");
        // 并行节点依赖于起始节点的输出
        builder.wire(INSTANCE_PROC_A, "startData", INSTANCE_START); // procA.startData <- startNode.output
        builder.wire(INSTANCE_PROC_B, "startData", INSTANCE_START); // procB.startData <- startNode.output
        builder.wire(INSTANCE_PROC_C, "startData", INSTANCE_START); // procC.startData <- startNode.output

        // 最终节点依赖于并行节点的输出
        builder.wire(INSTANCE_AGGREGATOR, "resultA", INSTANCE_PROC_A); // aggregator.resultA <- procA.output
        builder.wire(INSTANCE_AGGREGATOR, "resultB", INSTANCE_PROC_B); // aggregator.resultB <- procB.output
        builder.wire(INSTANCE_AGGREGATOR, "resultC", INSTANCE_PROC_C); // aggregator.resultC <- procC.output
        log.info("已定义所有节点连接。");

        // 4. 构建并返回不可变的 DagDefinition
        try {
            DagDefinition<ParalleContext> definition = builder.build();
            log.info("DataParallel DAG 定义构建成功！");
            return definition;
        } catch (IllegalStateException e) {
            log.error("构建 DataParallel DAG 定义失败: {}", e.getMessage(), e);
            // 在实际应用中，可能需要更健壮的错误处理
            throw new RuntimeException("Failed to build DataParallel DAG definition", e);
        }
    }
}
