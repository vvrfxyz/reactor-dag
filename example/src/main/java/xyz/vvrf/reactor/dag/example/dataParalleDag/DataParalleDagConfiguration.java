package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import xyz.vvrf.reactor.dag.builder.DagDefinitionBuilder;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.OutputSlot;
import xyz.vvrf.reactor.dag.example.dataParalleDag.node.*;
import xyz.vvrf.reactor.dag.monitor.web.service.DagDefinitionCache;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;
import xyz.vvrf.reactor.dag.registry.SpringScanningNodeRegistry; // 引入扫描注册表

import javax.annotation.Resource;

/**
 * DataParallel DAG 的 Spring 配置类 (适配简化后的框架)。
 * 职责：
 * 1. 定义 SpringScanningNodeRegistry<ParalleContext> Bean，自动注册节点。
 * 2. 定义 DagDefinition<ParalleContext> Bean。
 * 不再需要定义 NodeExecutor Bean 或手动注册节点。
 */
@Configuration
@Slf4j
public class DataParalleDagConfiguration {

    @Resource
    private DagDefinitionCache dagDefinitionCache;

    // 节点实例名称 (保持不变)
    private static final String INSTANCE_START = "startNode";
    private static final String INSTANCE_PROC_A = "parallelProcessorA";
    private static final String INSTANCE_PROC_B = "parallelProcessorB";
    private static final String INSTANCE_PROC_C = "parallelProcessorC";
    private static final String INSTANCE_AGGREGATOR = "finalAggregator";

    // 节点类型 ID (必须与 @DagNodeType 中的 id/value 匹配)
    private static final String TYPE_ID_START = "firstNodeType";
    private static final String TYPE_ID_PROC_A = "parallelNodeTypeA";
    private static final String TYPE_ID_PROC_B = "parallelNodeTypeB";
    private static final String TYPE_ID_PROC_C = "parallelNodeTypeC";
    private static final String TYPE_ID_AGGREGATOR = "finalNodeType";

    /**
     * 定义 ParalleContext 的 NodeRegistry Bean。
     * 使用 SpringScanningNodeRegistry 自动发现并注册带有 @DagNodeType(contextType=ParalleContext.class) 的节点 Bean。
     * @return NodeRegistry<ParalleContext> 实例。
     */
    @Bean
    public NodeRegistry<ParalleContext> paralleContextNodeRegistry() {
        log.info("创建 ParalleContext 的 SpringScanningNodeRegistry...");
        // 直接返回扫描注册表实例，它会在初始化时自动扫描
        return new SpringScanningNodeRegistry<>(ParalleContext.class);
    }

    /**
     * 定义 DataParallel DAG 的 DagDefinition Bean。
     * 使用上面定义的 NodeRegistry 来构建。
     * @param nodeRegistry 注入的 NodeRegistry<ParalleContext> Bean (现在是 SpringScanningNodeRegistry)。
     * @return DagDefinition<ParalleContext> 实例。
     */
    @Bean
    public DagDefinition<ParalleContext> dataParallelDagDefinition(NodeRegistry<ParalleContext> nodeRegistry) {

        log.info("开始构建 DataParallel DAG 定义...");
        DagDefinitionBuilder<ParalleContext> builder = new DagDefinitionBuilder<>(
                ParalleContext.class,
                "DataParallelDAG", // DAG 名称
                nodeRegistry       // 使用注入的 Registry
        );

        // 添加节点定义 (使用常量，这些常量必须与 @DagNodeType 中的 ID 匹配)
        log.info("添加节点定义...");
        builder.addNode(INSTANCE_START, TYPE_ID_START);
        builder.addNode(INSTANCE_PROC_A, TYPE_ID_PROC_A);
        builder.addNode(INSTANCE_PROC_B, TYPE_ID_PROC_B);
        builder.addNode(INSTANCE_PROC_C, TYPE_ID_PROC_C);
        builder.addNode(INSTANCE_AGGREGATOR, TYPE_ID_AGGREGATOR);
        log.info("已添加所有节点定义。");

        // 添加边定义 (使用常量和节点类中定义的 Slot)
        log.info("定义节点连接...");
        // Start -> Parallel A, B, C
        builder.addEdge(INSTANCE_START, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_PROC_A, ParallelNodeA.INPUT_START_DATA.getId());
        builder.addEdge(INSTANCE_START, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_PROC_B, ParallelNodeB.INPUT_START_DATA.getId());
        builder.addEdge(INSTANCE_START, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_PROC_C, ParallelNodeC.INPUT_START_DATA.getId());
        // Parallel A, B, C -> Aggregator
        builder.addEdge(INSTANCE_PROC_A, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_AGGREGATOR, FinalNode.INPUT_A.getId());
        builder.addEdge(INSTANCE_PROC_B, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_AGGREGATOR, FinalNode.INPUT_B.getId());
        builder.addEdge(INSTANCE_PROC_C, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_AGGREGATOR, FinalNode.INPUT_C.getId());
        log.info("已定义所有节点连接。");

        try {
            // 构建并返回不可变的 DAG 定义
            DagDefinition<ParalleContext> definition = builder.build();
            String dotString = builder.getLastGeneratedDotCode();
            String visJsString = builder.getLastGeneratedCytoscapeJsJson();
            dagDefinitionCache.cacheDag(definition, dotString, visJsString);
            log.info("DataParallel DAG 定义构建成功！");
            return definition;
        } catch (IllegalStateException e) {
            log.error("构建 DataParallel DAG 定义失败: {}", e.getMessage(), e);
            throw new RuntimeException("构建 DataParallel DAG 定义失败", e);
        }
    }

}
