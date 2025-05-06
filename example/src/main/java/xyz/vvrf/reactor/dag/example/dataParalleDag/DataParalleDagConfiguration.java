// file: example/dataParalleDag/DataParalleDagConfiguration.java (Refactored)
package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider; // 引入 ObjectProvider
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler; // 引入 Scheduler
import reactor.core.scheduler.Schedulers; // 引入 Schedulers
import xyz.vvrf.reactor.dag.builder.DagDefinitionBuilder;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.InputSlot;
import xyz.vvrf.reactor.dag.core.OutputSlot;
import xyz.vvrf.reactor.dag.example.dataParalleDag.node.*;
import xyz.vvrf.reactor.dag.execution.DagEngine; // 引入 DagEngine
import xyz.vvrf.reactor.dag.execution.NodeExecutor; // 引入 NodeExecutor
import xyz.vvrf.reactor.dag.execution.StandardDagEngine; // 引入 StandardDagEngine
import xyz.vvrf.reactor.dag.execution.StandardNodeExecutor; // 引入 StandardNodeExecutor
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener; // 引入 DagMonitorListener
import xyz.vvrf.reactor.dag.registry.NodeRegistry;
import xyz.vvrf.reactor.dag.registry.SimpleNodeRegistry;
import xyz.vvrf.reactor.dag.spring.SpringDagEngine; // 引入 SpringDagEngine
import xyz.vvrf.reactor.dag.spring.boot.DagFrameworkProperties; // 引入 DagFrameworkProperties

import java.util.Collections; // 引入 Collections
import java.util.List; // 引入 List
import java.util.Map;
import java.util.Objects;

@Configuration
@Slf4j
public class DataParalleDagConfiguration {

    // 节点实例名称 (保持不变)
    private static final String INSTANCE_START = "startNode";
    private static final String INSTANCE_PROC_A = "parallelProcessorA";
    private static final String INSTANCE_PROC_B = "parallelProcessorB";
    private static final String INSTANCE_PROC_C = "parallelProcessorC";
    private static final String INSTANCE_AGGREGATOR = "finalAggregator";

    // 节点类型 ID (保持不变)
    private static final String TYPE_ID_START = "firstNodeType";
    private static final String TYPE_ID_PROC_A = "parallelNodeTypeA";
    private static final String TYPE_ID_PROC_B = "parallelNodeTypeB";
    private static final String TYPE_ID_PROC_C = "parallelNodeTypeC";
    private static final String TYPE_ID_AGGREGATOR = "finalNodeType";

    private final ApplicationContext applicationContext;

    @Autowired
    public DataParalleDagConfiguration(ApplicationContext applicationContext) {
        this.applicationContext = Objects.requireNonNull(applicationContext);
    }

    @Bean
    public NodeRegistry<ParalleContext> paralleContextNodeRegistry() {
        log.info("创建 ParalleContext 的 NodeRegistry...");
        SimpleNodeRegistry<ParalleContext> registry = new SimpleNodeRegistry<>();
        Map<String, DagNode> nodeBeans = applicationContext.getBeansOfType(DagNode.class);

        log.info("发现 {} 个 DagNode Bean，尝试注册...", nodeBeans.size());
        nodeBeans.forEach((beanName, nodeInstance) -> {
            try {
                if (isAssignableToDagNodeParalleContext(nodeInstance)) {
                    log.debug("注册 Node Type ID: '{}' using Bean: {}", beanName, nodeInstance.getClass().getSimpleName());
                    registry.register(beanName, nodeInstance);
                } else {
                    log.warn("跳过注册 Bean '{}'，因为它不适用于 ParalleContext。", beanName);
                }
            } catch (IllegalArgumentException e) {
                log.warn("注册 Node Type ID '{}' 失败 (可能已存在): {}", beanName, e.getMessage());
            } catch (Exception e) {
                log.error("注册 Node Type ID '{}' 时发生意外错误", beanName, e);
            }
        });
        log.info("NodeRegistry<ParalleContext> 创建并填充完毕。");
        return registry;
    }

    // 辅助方法检查类型 (简化版)
    private boolean isAssignableToDagNodeParalleContext(Object bean) {
        // 实际应用中可能需要更健壮的检查
        return bean instanceof FirstNode || bean instanceof ParallelNodeA || bean instanceof ParallelNodeB || bean instanceof ParallelNodeC || bean instanceof FinalNode;
    }

    @Bean
    public DagDefinition<ParalleContext> dataParallelDagDefinition(NodeRegistry<ParalleContext> nodeRegistry) {
        log.info("开始构建 DataParallel DAG 定义 (使用 NodeRegistry)...");
        DagDefinitionBuilder<ParalleContext> builder = new DagDefinitionBuilder<>(
                ParalleContext.class,
                "DataParallelDAG",
                nodeRegistry
        );
        // ... (addNode 和 addEdge 调用保持不变)
        log.info("添加节点定义...");
        builder.addNode(INSTANCE_START, TYPE_ID_START);
        builder.addNode(INSTANCE_PROC_A, TYPE_ID_PROC_A);
        builder.addNode(INSTANCE_PROC_B, TYPE_ID_PROC_B);
        builder.addNode(INSTANCE_PROC_C, TYPE_ID_PROC_C);
        builder.addNode(INSTANCE_AGGREGATOR, TYPE_ID_AGGREGATOR);
        log.info("已添加所有节点定义。");

        log.info("定义节点连接...");
        builder.addEdge(INSTANCE_START, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_PROC_A, ParallelNodeA.INPUT_START_DATA.getId());
        builder.addEdge(INSTANCE_START, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_PROC_B, ParallelNodeB.INPUT_START_DATA.getId());
        builder.addEdge(INSTANCE_START, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_PROC_C, ParallelNodeC.INPUT_START_DATA.getId());
        builder.addEdge(INSTANCE_PROC_A, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_AGGREGATOR, FinalNode.INPUT_A.getId());
        builder.addEdge(INSTANCE_PROC_B, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_AGGREGATOR, FinalNode.INPUT_B.getId());
        builder.addEdge(INSTANCE_PROC_C, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, INSTANCE_AGGREGATOR, FinalNode.INPUT_C.getId());
        log.info("已定义所有节点连接。");

        try {
            DagDefinition<ParalleContext> definition = builder.build();
            log.info("DataParallel DAG 定义构建成功！");
            return definition;
        } catch (IllegalStateException e) {
            log.error("构建 DataParallel DAG 定义失败: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to build DataParallel DAG definition", e);
        }
    }

    // --- 手动配置核心组件 Bean ---

    /**
     * 创建特定于 ParalleContext 的 NodeExecutor Bean。
     */
    @Bean
    public NodeExecutor<ParalleContext> paralleContextNodeExecutor(
            NodeRegistry<ParalleContext> registry, // 注入特定上下文的 Registry
            DagFrameworkProperties properties,     // 注入配置属性
            ObjectProvider<Scheduler> schedulerProvider, // 可选注入调度器
            ObjectProvider<List<DagMonitorListener>> listenersProvider // 可选注入监听器
    ) {
        log.info("创建 ParalleContext 的 NodeExecutor...");
        Scheduler scheduler = schedulerProvider.getIfAvailable(Schedulers::boundedElastic);
        List<DagMonitorListener> listeners = listenersProvider.getIfAvailable(Collections::emptyList);
        return new StandardNodeExecutor<>( // 使用 StandardNodeExecutor 实现
                registry,
                properties.getNode().getDefaultTimeout(),
                scheduler,
                listeners
        );
    }

    /**
     * 创建特定于 ParalleContext 的核心 DagEngine Bean。
     */
    @Bean
    public DagEngine<ParalleContext> paralleContextDagEngine( // 返回核心接口类型
                                                              NodeRegistry<ParalleContext> registry,     // 注入 Registry
                                                              NodeExecutor<ParalleContext> executor,     // 注入上面创建的 Executor
                                                              DagFrameworkProperties properties          // 注入配置属性
    ) {
        log.info("创建 ParalleContext 的 DagEngine...");
        return new StandardDagEngine<>( // 使用 StandardDagEngine 实现
                registry,
                executor,
                properties.getEngine().getConcurrencyLevel()
        );
    }

    /**
     * 创建特定于 ParalleContext 的 SpringDagEngine Bean。
     * 这个 Bean 将被 DagUsageExample 注入。
     */
    @Bean
    public SpringDagEngine<ParalleContext> paralleContextSpringDagEngine(
            DagEngine<ParalleContext> dagEngine // 注入上面创建的核心引擎
    ) {
        log.info("创建 ParalleContext 的 SpringDagEngine...");
        return new SpringDagEngine<>(dagEngine);
    }
}
