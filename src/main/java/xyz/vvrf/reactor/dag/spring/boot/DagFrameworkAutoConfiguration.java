// 文件名: spring/boot/DagFrameworkAutoConfiguration.java (已修改 - 仅注释)
package xyz.vvrf.reactor.dag.spring.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reactor DAG 框架的 Spring Boot 自动配置类 (已重构)。
 * 职责:
 * 1. 启用并绑定 {@link DagFrameworkProperties}。
 * 2. 提供核心的 {@link DagEngineProvider} Bean，它处理引擎的创建。
 * 3. 提供可选的默认 {@link Scheduler} Bean ("dagNodeExecutionScheduler")。
 * 4. 收集所有的 {@link DagMonitorListener} Bean 到一个列表 Bean ("dagMonitorListeners")。
 * <p>
 * **用户职责:**
 * 1. 实现你的业务逻辑 {@link xyz.vvrf.reactor.dag.core.DagNode}。
 * 2. 使用 {@link xyz.vvrf.reactor.dag.annotation.DagNodeType} 注解实现类，指定 ID 和 contextType。
 * 3. 为每个业务上下文 `MyContext`，定义一个 {@link xyz.vvrf.reactor.dag.registry.NodeRegistry<MyContext>} Bean
 *    (通常是 {@link xyz.vvrf.reactor.dag.registry.SpringScanningNodeRegistry<MyContext>})。
 * 4. 在需要执行 DAG 的地方注入 {@link DagEngineProvider} 并调用 {@code getEngine(MyContext.class)}。
 * 5. 使用 {@link xyz.vvrf.reactor.dag.builder.DagDefinitionBuilder} (注入相应的 NodeRegistry) 来构建 DAG 定义。
 *
 * **注意:** 用户不再需要显式定义 {@code NodeExecutor<C>} Bean。
 *
 * @author Refactored
 */
@Configuration
@EnableConfigurationProperties(DagFrameworkProperties.class)
@Slf4j
public class DagFrameworkAutoConfiguration {

    public DagFrameworkAutoConfiguration() {
        log.info("Reactor DAG 框架自动配置 (DagFrameworkAutoConfiguration) 已加载。");
    }

    /**
     * 提供核心的 DagEngineProvider Bean。
     * 它管理并提供不同上下文类型的 DagEngine 实例，
     * 自动发现所需的 NodeRegistry Bean 并内部创建 NodeExecutor。
     *
     * @param properties 自动注入的框架配置属性。
     * @return DagEngineProvider 实例。
     */
    @Bean
    @ConditionalOnMissingBean(DagEngineProvider.class)
    public DagEngineProvider dagEngineProvider(DagFrameworkProperties properties) {
        log.info("正在创建 DagEngineProvider Bean...");
        return new DagEngineProvider(properties);
    }

    /**
     * 提供一个默认的 Reactor Scheduler Bean 用于节点执行。
     * 如果已存在名为 "dagNodeExecutionScheduler" 的 Bean，则不创建此默认 Bean。
     * 默认使用 Schedulers.boundedElastic()。
     *
     * @param properties 框架属性 (未来可能用于配置调度器)。
     * @return 默认的节点执行 Scheduler。
     */
    @Bean(name = "dagNodeExecutionScheduler")
    @ConditionalOnMissingBean(name = "dagNodeExecutionScheduler")
    public Scheduler dagNodeExecutionScheduler(DagFrameworkProperties properties) {
        log.info("正在创建默认节点执行调度器 'dagNodeExecutionScheduler' (Schedulers.boundedElastic())...");
        // 未来: 可以使用 properties 配置类型、大小等。
        return Schedulers.boundedElastic();
    }

    /**
     * 收集在应用上下文中定义的所有 DagMonitorListener Bean。
     * 将它们作为名为 "dagMonitorListeners" 的不可变列表 Bean 提供。
     * 此列表由 DagEngineProvider 内部创建的 StandardNodeExecutor 使用。
     *
     * @param listenersProvider Spring 的 DagMonitorListener Bean 的 ObjectProvider。
     * @return 包含所有找到的 DagMonitorListener Bean 的不可变列表，如果适用则有序。
     */
    @Bean(name = "dagMonitorListeners")
    @ConditionalOnMissingBean(name = "dagMonitorListeners")
    public List<DagMonitorListener> dagMonitorListeners(ObjectProvider<DagMonitorListener> listenersProvider) {
        log.info("正在收集 DagMonitorListener Bean...");
        List<DagMonitorListener> listeners = listenersProvider.orderedStream().collect(Collectors.toList());
        if (listeners.isEmpty()) {
            log.info("在 Spring 上下文中未找到 DagMonitorListener Bean。");
        } else {
            log.info("收集到 {} 个 DagMonitorListener Bean: {}", listeners.size(),
                    listeners.stream().map(l -> l.getClass().getSimpleName()).collect(Collectors.joining(", ")));
        }
        return Collections.unmodifiableList(listeners);
    }
}
