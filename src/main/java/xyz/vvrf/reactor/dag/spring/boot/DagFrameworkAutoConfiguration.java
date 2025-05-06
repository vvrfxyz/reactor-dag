package xyz.vvrf.reactor.dag.spring.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import; // 引入 Import
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reactor DAG 框架的 Spring Boot 自动配置类 (重构后)。
 * 主要职责：
 * 1. 启用并绑定 {@link DagFrameworkProperties}。
 * 2. 提供核心的 {@link DagEngineProvider} Bean，它负责自动发现和装配引擎。
 * 3. 提供可选的默认 {@link Scheduler} Bean (名为 "dagNodeExecutionScheduler") 用于节点执行。
 * 4. 收集所有 {@link DagMonitorListener} Bean 并提供一个列表 Bean (名为 "dagMonitorListeners")。
 * <p>
 * **用户需要做的事情：**
 * 1. 为你的每个业务上下文类型 `MyContext` 定义一个 `NodeRegistry<MyContext>` Bean。
 * 2. 为你的每个业务上下文类型 `MyContext` 定义一个 `NodeExecutor<MyContext>` Bean。
 * (通常使用 `StandardNodeExecutor<MyContext>`)
 * 3. 在需要执行 DAG 的地方，注入 `DagEngineProvider` 并调用 `getEngine(MyContext.class)` 获取引擎。
 *
 * @author Refactored (核心重构)
 */
@Configuration
// 启用属性绑定，确保 DagFrameworkProperties Bean 可用并已填充属性
@EnableConfigurationProperties(DagFrameworkProperties.class)
// 可以考虑将 Provider 的创建移到这里，而不是单独的 @Bean 方法，以确保 Properties 可用
// @Import(DagEngineProvider.class) // 或者使用 @Import 导入 Provider
@Slf4j
public class DagFrameworkAutoConfiguration {

    public DagFrameworkAutoConfiguration() {
        log.info("Reactor DAG 框架自动配置 (DagFrameworkAutoConfiguration) 已加载。");
    }

    /**
     * 提供核心的 DagEngineProvider Bean。
     * 它负责管理和提供不同上下文类型的 DagEngine 实例，并自动发现所需的 Registry 和 Executor。
     * 确保在需要时才创建 (默认单例)。
     *
     * @param properties 自动注入的框架配置属性。
     * @return DagEngineProvider 实例。
     */
    @Bean
    @ConditionalOnMissingBean(DagEngineProvider.class) // 仅在用户未提供自定义 Provider 时创建
    public DagEngineProvider dagEngineProvider(DagFrameworkProperties properties) {
        log.info("正在创建 DagEngineProvider Bean...");
        return new DagEngineProvider(properties);
    }

    /**
     * 提供一个默认的 Reactor Scheduler Bean，用于节点执行。
     * 如果用户定义了自己的名为 "dagNodeExecutionScheduler" 的 Scheduler Bean，则此默认 Bean 不会创建。
     * 使用 Schedulers.boundedElastic() 作为默认值，适合 I/O 密集型或混合型任务。
     *
     * @return 默认的节点执行调度器 Scheduler。
     */
    @Bean(name = "dagNodeExecutionScheduler") // 指定 Bean 名称，方便用户注入
    @ConditionalOnMissingBean(name = "dagNodeExecutionScheduler")
    public Scheduler dagNodeExecutionScheduler(DagFrameworkProperties properties) {
        // 可以考虑未来根据 properties 配置选择不同的 Scheduler 类型
        log.info("正在创建默认的节点执行调度器 'dagNodeExecutionScheduler' (Schedulers.boundedElastic())...");
        return Schedulers.boundedElastic();
    }

    /**
     * 收集应用上下文中所有的 DagMonitorListener Bean。
     * 这个 Bean 本身是一个 List<DagMonitorListener>，可以被用户定义的 NodeExecutor Bean 注入。
     * 使用 ObjectProvider 来安全地处理没有监听器的情况，并支持排序。
     *
     * @param listenersProvider Spring 提供的 ObjectProvider，包含所有 DagMonitorListener 类型的 Bean。
     * @return 一个包含所有找到的 DagMonitorListener 的不可变列表，如果找不到则为空列表。
     */
    @Bean(name = "dagMonitorListeners") // 提供一个明确的 Bean 名称
    @ConditionalOnMissingBean(name = "dagMonitorListeners")
    public List<DagMonitorListener> dagMonitorListeners(ObjectProvider<DagMonitorListener> listenersProvider) {
        log.info("正在收集 DagMonitorListener Beans...");
        // 使用 orderedStream() 获取按 @Order 或 Ordered 接口排序的监听器流
        List<DagMonitorListener> listeners = listenersProvider.orderedStream().collect(Collectors.toList());
        if (listeners.isEmpty()) {
            log.info("在 Spring 上下文中未找到 DagMonitorListener Bean。");
        } else {
            log.info("已收集到 {} 个 DagMonitorListener Bean: {}", listeners.size(),
                    listeners.stream().map(l -> l.getClass().getSimpleName()).collect(Collectors.joining(", ")));
        }
        // 返回不可变列表
        return Collections.unmodifiableList(listeners);
    }
}

// --- 用户配置示例 (更新后的简化版) ---
//    /*
//    package com.example.myapp.config;
//
//    import com.example.myapp.dag.*; // 包含 MyContext, MyNode1, MyNode2 等
//    import lombok.extern.slf4j.Slf4j;
//    import org.springframework.beans.factory.annotation.Qualifier;
//    import org.springframework.context.annotation.Bean;
//    import org.springframework.context.annotation.Configuration;
//    import reactor.core.scheduler.Scheduler;
//    import xyz.vvrf.reactor.dag.execution.NodeExecutor;
//    import xyz.vvrf.reactor.dag.execution.StandardNodeExecutor;
//    import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;
//    import xyz.vvrf.reactor.dag.registry.NodeRegistry;
//    import xyz.vvrf.reactor.dag.registry.SimpleNodeRegistry;
//    import xyz.vvrf.reactor.dag.spring.boot.DagFrameworkProperties;
//
//    import java.util.List;
//
//    @Configuration
//    @Slf4j
//    public class MyDagBeansConfiguration {
//
//        // 1. 定义你的 NodeRegistry<MyContext> Bean
//        @Bean
//        public NodeRegistry<MyContext> myContextNodeRegistry() {
//            log.info("正在创建 MyContext 的 NodeRegistry...");
//            SimpleNodeRegistry<MyContext> registry = new SimpleNodeRegistry<>(MyContext.class);
//
//            // 在这里注册你的 DagNode 实现 (原型或工厂)
//            registry.register("nodeTypeA", new MyNode1()); // 假设 MyNode1 是无状态原型
//            registry.register("nodeTypeB", () -> new MyNode2("依赖注入的服务")); // 假设 MyNode2 需要依赖
//
//            log.info("MyContext NodeRegistry 已配置。");
//            return registry;
//        }
//
//        // 2. 定义你的 NodeExecutor<MyContext> Bean
//        @Bean
//        public NodeExecutor<MyContext> myContextNodeExecutor(
//                NodeRegistry<MyContext> registry, // Spring 会自动注入上面定义的 Bean
//                DagFrameworkProperties properties, // 注入框架配置
//                @Qualifier("dagNodeExecutionScheduler") Scheduler scheduler, // 注入自动配置的或自定义的调度器
//                @Qualifier("dagMonitorListeners") List<DagMonitorListener> monitorListeners // 注入自动收集的监听器列表
//        ) {
//            log.info("正在创建 MyContext 的 StandardNodeExecutor...");
//            // 使用 StandardNodeExecutor 或自定义实现
//            return new StandardNodeExecutor<>(
//                    registry,
//                    properties.getNode().getDefaultTimeout(), // 使用配置的默认超时
//                    scheduler,
//                    monitorListeners
//            );
//        }
//
//        // 3. (可选) 定义其他上下文的 Registry 和 Executor Beans...
//        // @Bean
//        // public NodeRegistry<AnotherContext> anotherContextNodeRegistry() { ... }
//        // @Bean
//        // public NodeExecutor<AnotherContext> anotherContextNodeExecutor(...) { ... }
//
//    }
//
//    // --- 在服务中使用 ---
//    package com.example.myapp.service;
//
//    import com.example.myapp.config.MyContext;
//    import org.springframework.beans.factory.annotation.Autowired;
//    import org.springframework.stereotype.Service;
//    import reactor.core.publisher.Flux;
//    import xyz.vvrf.reactor.dag.builder.DagDefinitionBuilder;
//    import xyz.vvrf.reactor.dag.core.DagDefinition;
//    import xyz.vvrf.reactor.dag.core.Event;
//    import xyz.vvrf.reactor.dag.execution.DagEngine;
//    import xyz.vvrf.reactor.dag.registry.NodeRegistry;
//    import xyz.vvrf.reactor.dag.spring.boot.DagEngineProvider; // 注入 Provider
//
//    @Service
//    @Slf4j
//    public class MyDagService {
//
//        @Autowired
//        private DagEngineProvider dagEngineProvider; // 注入 Provider
//
//        @Autowired
//        private NodeRegistry<MyContext> myContextRegistry; // 也可以直接注入 Registry 用于构建 Definition
//
//        public void runMySpecificDag() {
//            // 1. 获取特定上下文的引擎
//            DagEngine<MyContext> engine = dagEngineProvider.getEngine(MyContext.class);
//
//            // 2. 构建 DagDefinition (需要对应的 Registry)
//            DagDefinitionBuilder<MyContext> builder = new DagDefinitionBuilder<>(
//                MyContext.class, "my-sample-dag", myContextRegistry // 使用注入的 Registry
//            );
//
//            builder.addNode("startNode", "nodeTypeA")
//                   .addNode("endNode", "nodeTypeB")
//                   .addEdge("startNode", "endNode", "inputB"); // 假设 nodeTypeB 有输入槽 "inputB"
//
//            DagDefinition<MyContext> definition = builder.build();
//
//            // 3. 准备初始上下文并执行
//            MyContext initialContext = new MyContext(/* ... */);
//Flux<Event<?>> eventFlux = engine.execute(initialContext, definition);
//
//// 4. 处理结果 (订阅事件流)
//            eventFlux.subscribe(
//        event -> log.info("收到 DAG 事件: {}", event),
//error -> log.error("DAG 执行失败", error),
//                () -> log.info("DAG 执行成功完成。")
//            );
//                    }
//                    }
//
