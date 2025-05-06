// file: spring/boot/DagFrameworkAutoConfiguration.java
package xyz.vvrf.reactor.dag.spring.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reactor DAG 框架的 Spring Boot 自动配置类。
 * 主要负责：
 * 1. 启用并绑定 {@link DagFrameworkProperties}。
 * 2. 提供核心的 {@link DagEngineProvider} Bean。
 * 3. 提供可选的默认 {@link Scheduler} Bean 用于节点执行。
 * 4. 收集所有 {@link DagMonitorListener} Bean。
 *
 * 用户需要自行定义特定上下文类型 C 的 {@link xyz.vvrf.reactor.dag.registry.NodeRegistry<C>}
 * 和 {@link xyz.vvrf.reactor.dag.execution.NodeExecutor<C>} Beans。
 *
 * @author Refactored
 */
@Configuration
@EnableConfigurationProperties(DagFrameworkProperties.class) // 启用属性绑定
@Slf4j
public class DagFrameworkAutoConfiguration {

    public DagFrameworkAutoConfiguration() {
        log.info("DagFrameworkAutoConfiguration loaded.");
    }

    /**
     * 提供核心的 DagEngineProvider Bean。
     * 它负责管理和提供不同上下文类型的 DagEngine 实例。
     * 使用 @Lazy 注解，因为它需要 ApplicationContext，确保在上下文完全初始化后创建。
     */
    @Bean
    @ConditionalOnMissingBean(DagEngineProvider.class)
    @Lazy // 延迟初始化，因为它依赖 ApplicationContext
    public DagEngineProvider dagEngineProvider(DagFrameworkProperties properties) {
        return new DagEngineProvider(properties);
    }

    /**
     * 提供一个默认的 Reactor Scheduler Bean，用于节点执行。
     * 如果用户定义了自己的名为 "dagNodeExecutionScheduler" 的 Scheduler Bean，则此默认 Bean 不会创建。
     * 使用 Schedulers.boundedElastic() 作为默认值，适合 I/O 密集型或混合型任务。
     */
    @Bean(name = "dagNodeExecutionScheduler") // 指定 Bean 名称
    @ConditionalOnMissingBean(name = "dagNodeExecutionScheduler")
    public Scheduler dagNodeExecutionScheduler() {
        log.info("Creating default dagNodeExecutionScheduler (Schedulers.boundedElastic()).");
        return Schedulers.boundedElastic();
    }

    /**
     * 收集应用上下文中所有的 DagMonitorListener Bean。
     * 这个 Bean 本身是一个 List<DagMonitorListener>，可以被用户定义的 NodeExecutor Bean 注入。
     * 使用 ObjectProvider 来安全地处理没有监听器的情况。
     *
     * @param listenersProvider Spring 提供的 ObjectProvider，包含所有 DagMonitorListener 类型的 Bean。
     * @return 一个包含所有找到的 DagMonitorListener 的列表，如果找不到则为空列表。
     */
    @Bean
    @ConditionalOnMissingBean(name = "dagMonitorListeners") // 提供一个明确的 Bean
    public List<DagMonitorListener> dagMonitorListeners(ObjectProvider<DagMonitorListener> listenersProvider) {
        List<DagMonitorListener> listeners = listenersProvider.orderedStream().collect(Collectors.toList());
        if (listeners.isEmpty()) {
            log.info("No DagMonitorListener beans found in the context.");
        } else {
            log.info("Collected {} DagMonitorListener bean(s): {}", listeners.size(),
                    listeners.stream().map(l -> l.getClass().getSimpleName()).collect(Collectors.joining(", ")));
        }
        // 返回不可变列表
        return Collections.unmodifiableList(listeners);
    }

    // --- 用户配置示例 (注释掉，放在这里作为文档) ---
    /*
    @Configuration
    class MyDagConfiguration {

        // 1. 定义你的上下文类型
        // public static class MyContext { ... }

        // 2. 定义你的 NodeRegistry Bean
        @Bean
        public NodeRegistry<MyContext> myNodeRegistry() {
            SimpleNodeRegistry<MyContext> registry = new SimpleNodeRegistry<>(MyContext.class);
            // 在这里注册你的 DagNode 实现
            // registry.register("myNodeType1", new MyNode1());
            // registry.register("myNodeType2", () -> new MyNode2());
            log.info("MyContext NodeRegistry created and configured.");
            return registry;
        }

        // 3. 定义你的 NodeExecutor Bean
        @Bean
        public NodeExecutor<MyContext> myNodeExecutor(
                NodeRegistry<MyContext> myNodeRegistry, // 注入对应的 Registry
                DagFrameworkProperties properties,
                @Qualifier("dagNodeExecutionScheduler") Scheduler scheduler, // 注入自动配置的 Scheduler
                List<DagMonitorListener> monitorListeners // 注入收集的 Listeners
        ) {
            log.info("Creating StandardNodeExecutor for MyContext.");
            return new StandardNodeExecutor<>(
                    myNodeRegistry,
                    properties.getNode().getDefaultTimeout(),
                    scheduler,
                    monitorListeners
            );
        }

        // 4. 在你的服务或组件中注入 DagEngineProvider
        @Autowired
        private DagEngineProvider dagEngineProvider;

        public void runMyDag() {
            // 5. 获取特定上下文的引擎
            DagEngine<MyContext> engine = dagEngineProvider.getEngine(MyContext.class);

            // 6. 构建 DagDefinition (需要对应的 Registry)
            DagDefinitionBuilder<MyContext> builder = new DagDefinitionBuilder<>(
                MyContext.class, "my-specific-dag", myNodeRegistry() // 或者注入 Registry Bean
            );
            // ... 添加节点和边 ...
            DagDefinition<MyContext> definition = builder.build();

            // 7. 执行 DAG
            MyContext initialContext = new MyContext();
            Flux<Event<?>> eventFlux = engine.execute(initialContext, definition);
            eventFlux.subscribe(
                event -> log.info("Received DAG event: {}", event),
                error -> log.error("DAG execution failed", error),
                () -> log.info("DAG execution completed.")
            );
        }
    }
    */
}
