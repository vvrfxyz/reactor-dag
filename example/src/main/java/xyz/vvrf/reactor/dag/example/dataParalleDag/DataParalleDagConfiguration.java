package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier; // 引入 Qualifier
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.builder.DagDefinitionBuilder;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.OutputSlot;
import xyz.vvrf.reactor.dag.example.dataParalleDag.node.*;
// 移除不再需要的 DagEngine 和 SpringDagEngine 导入
import xyz.vvrf.reactor.dag.execution.NodeExecutor;
import xyz.vvrf.reactor.dag.execution.StandardNodeExecutor;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;
import xyz.vvrf.reactor.dag.registry.SimpleNodeRegistry;
import xyz.vvrf.reactor.dag.spring.boot.DagFrameworkProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * DataParallel DAG 的 Spring 配置类 (适配简化后的框架)。
 * 职责：
 * 1. 定义 NodeRegistry<ParalleContext> Bean，并注册节点实现。
 * 2. 定义 NodeExecutor<ParalleContext> Bean。
 * 3. 定义 DagDefinition<ParalleContext> Bean。
 * 不再需要手动创建 DagEngine 或 SpringDagEngine Bean。
 */
@Configuration
@Slf4j
public class DataParalleDagConfiguration {

    // 节点实例名称 (保持不变)
    private static final String INSTANCE_START = "startNode";
    private static final String INSTANCE_PROC_A = "parallelProcessorA";
    private static final String INSTANCE_PROC_B = "parallelProcessorB";
    private static final String INSTANCE_PROC_C = "parallelProcessorC";
    private static final String INSTANCE_AGGREGATOR = "finalAggregator";

    // 节点类型 ID (与 @Component 名称对应)
    private static final String TYPE_ID_START = "firstNodeType";
    private static final String TYPE_ID_PROC_A = "parallelNodeTypeA";
    private static final String TYPE_ID_PROC_B = "parallelNodeTypeB";
    private static final String TYPE_ID_PROC_C = "parallelNodeTypeC";
    private static final String TYPE_ID_AGGREGATOR = "finalNodeType";

    // 注入 ApplicationContext 用于查找节点 Bean
    private final ApplicationContext applicationContext;

    @Autowired
    public DataParalleDagConfiguration(ApplicationContext applicationContext) {
        this.applicationContext = Objects.requireNonNull(applicationContext);
    }

    /**
     * 定义 ParalleContext 的 NodeRegistry Bean。
     * 它会自动查找 ApplicationContext 中适用于 ParalleContext 的 DagNode Bean 并注册。
     * @return NodeRegistry<ParalleContext> 实例。
     */
    @Bean
    public NodeRegistry<ParalleContext> paralleContextNodeRegistry() {
        log.info("创建 ParalleContext 的 NodeRegistry...");
        // 使用 SimpleNodeRegistry 实现
        SimpleNodeRegistry<ParalleContext> registry = new SimpleNodeRegistry<>(ParalleContext.class);

        // 查找所有 DagNode 类型的 Bean
        Map<String, DagNode> nodeBeans = applicationContext.getBeansOfType(DagNode.class);

        log.info("发现 {} 个 DagNode Bean，尝试注册到 ParalleContext Registry...", nodeBeans.size());
        nodeBeans.forEach((beanName, nodeInstance) -> {
            // 检查 Bean 是否适用于 ParalleContext
            // 注意：这里的检查逻辑比较简单，实际项目中可能需要更精确的方式
            // 例如，通过自定义注解或检查节点的泛型类型参数
            if (isNodeForParalleContext(nodeInstance)) {
                try {
                    // 使用 Bean 的名称作为 Node Type ID 进行注册
                    // 确保 @Component("...") 的值与这里使用的 beanName 一致
                    log.debug("注册节点类型 ID: '{}' (来自 Bean: {}, 实现: {})",
                            beanName, beanName, nodeInstance.getClass().getSimpleName());
                    // 强制类型转换，因为 isNodeForParalleContext 保证了类型
                    @SuppressWarnings("unchecked")
                    DagNode<ParalleContext, ?> typedNode = (DagNode<ParalleContext, ?>) nodeInstance;
                    registry.register(beanName, typedNode); // 注册原型
                } catch (IllegalArgumentException e) {
                    // 捕获重复注册的异常
                    log.warn("注册节点类型 ID '{}' 失败 (可能已存在): {}", beanName, e.getMessage());
                } catch (Exception e) {
                    // 捕获其他注册时可能发生的异常
                    log.error("注册节点类型 ID '{}' 时发生意外错误", beanName, e);
                }
            } else {
                log.trace("跳过注册 Bean '{}' (类型: {}),因为它不适用于 ParalleContext。",
                        beanName, nodeInstance.getClass().getSimpleName());
            }
        });
        log.info("NodeRegistry<ParalleContext> 创建并填充完毕。");
        return registry;
    }

    // 辅助方法：检查节点 Bean 是否适用于 ParalleContext
    // 这是一个简化的示例检查，实际应用可能需要更健壮的方法
    private boolean isNodeForParalleContext(Object nodeInstance) {
        // 可以基于 instanceof 检查，或者检查类上的注解，或者检查泛型类型
        return nodeInstance instanceof FirstNode ||
                nodeInstance instanceof ParallelNodeA ||
                nodeInstance instanceof ParallelNodeB ||
                nodeInstance instanceof ParallelNodeC ||
                nodeInstance instanceof FinalNode;
        // 更健壮的方式可能是检查 nodeInstance.getClass().getGenericInterfaces() 来判断 DagNode<C,P> 中的 C 是否是 ParalleContext
    }

    /**
     * 定义 DataParallel DAG 的 DagDefinition Bean。
     * 使用上面定义的 NodeRegistry 来构建。
     * @param nodeRegistry 注入的 NodeRegistry<ParalleContext> Bean。
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

        // 添加节点定义 (使用常量)
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
            log.info("DataParallel DAG 定义构建成功！");
            return definition;
        } catch (IllegalStateException e) {
            // 如果构建失败（例如，验证错误、循环等），记录错误并抛出运行时异常
            log.error("构建 DataParallel DAG 定义失败: {}", e.getMessage(), e);
            // 在 Spring 环境中，这通常会导致应用启动失败，这是期望的行为
            throw new RuntimeException("构建 DataParallel DAG 定义失败", e);
        }
    }

    /**
     * 定义特定于 ParalleContext 的 NodeExecutor Bean。
     * 这是框架运行 DAG 所必需的。
     *
     * @param registry 注入特定上下文的 NodeRegistry<ParalleContext> Bean。
     * @param properties 注入框架配置属性。
     * @param schedulerProvider 用于获取节点执行调度器的 ObjectProvider (来自框架自动配置或用户自定义)。
     * @param listenersProvider 用于获取监控监听器列表的 ObjectProvider (来自框架自动配置)。
     * @return NodeExecutor<ParalleContext> 实例。
     */
    @Bean
    public NodeExecutor<ParalleContext> paralleContextNodeExecutor(
            NodeRegistry<ParalleContext> registry,
            DagFrameworkProperties properties,
            // 使用 @Qualifier 指定 Bean 名称，确保注入正确的 Scheduler 和 Listener 列表
            @Qualifier("dagNodeExecutionScheduler") ObjectProvider<Scheduler> schedulerProvider,
            @Qualifier("dagMonitorListeners") ObjectProvider<List<DagMonitorListener>> listenersProvider
    ) {
        log.info("创建 ParalleContext 的 NodeExecutor...");
        // 从 ObjectProvider 获取依赖，如果不存在则使用默认值
        Scheduler scheduler = schedulerProvider.getIfAvailable(Schedulers::boundedElastic); // 默认使用有界弹性调度器
        List<DagMonitorListener> listeners = listenersProvider.getIfAvailable(Collections::emptyList); // 默认空列表

        log.debug("NodeExecutor<ParalleContext> 使用调度器: {}, 监听器数量: {}",
                scheduler.getClass().getSimpleName(), listeners.size());

        // 使用 StandardNodeExecutor 实现
        return new StandardNodeExecutor<>(
                registry,
                properties.getNode().getDefaultTimeout(), // 使用配置的默认超时
                scheduler,
                listeners
        );
    }

}
