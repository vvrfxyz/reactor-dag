package xyz.vvrf.reactor.dag.spring.boot;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.impl.StandardNodeExecutor;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;
import xyz.vvrf.reactor.dag.spring.SpringDagEngine;

import java.util.Collections;
import java.util.List;

/**
 * Reactor DAG框架的Spring Boot自动配置类。
 * 基于 DagFrameworkProperties 配置框架。
 * 如果用户未定义，则提供默认的 Bean。
 *
 * @author ruifeng.wen (modified)
 */
@Configuration
@EnableConfigurationProperties(DagFrameworkProperties.class)
public class DagFrameworkAutoConfiguration {

    /**
     * 如果上下文中不存在 StandardNodeExecutor Bean，则提供一个默认的实例。
     * 使用 DagFrameworkProperties 进行配置。
     * 允许用户自定义用于节点执行的 Scheduler Bean。
     * 注入所有可用的 DagMonitorListener beans。
     *
     * @param properties                  配置属性。
     * @param nodeExecutionSchedulerProvider 可选的自定义 Scheduler 提供者。
     * @param monitorListenersProvider    可选的 DagMonitorListener bean 列表提供者。
     * Spring 会自动收集此类型的所有 bean。
     * @return StandardNodeExecutor 实例。
     */
    @Bean
    @ConditionalOnMissingBean
    public StandardNodeExecutor standardNodeExecutor(
            DagFrameworkProperties properties,
            ObjectProvider<Scheduler> nodeExecutionSchedulerProvider,
            ObjectProvider<List<DagMonitorListener>> monitorListenersProvider
    ) {
        // StandardNodeExecutor 的构造函数未变，所以这里无需修改
        Scheduler scheduler = nodeExecutionSchedulerProvider.getIfAvailable(Schedulers::boundedElastic);
        List<DagMonitorListener> listeners = monitorListenersProvider.getIfAvailable(Collections::emptyList);

        return new StandardNodeExecutor(
                properties.getNode().getDefaultTimeout(),
                scheduler,
                listeners
        );
    }

    /**
     * 如果上下文中不存在 SpringDagEngine Bean，则提供一个默认的实例。
     * 使用 DagFrameworkProperties 进行配置。
     *
     * @param nodeExecutor StandardNodeExecutor Bean (默认的或用户自定义的)。
     * @param properties   绑定的配置属性。
     * @return SpringDagEngine 实例。
     */
    @Bean
    @ConditionalOnMissingBean
    public SpringDagEngine springDagEngine(
            StandardNodeExecutor nodeExecutor,
            DagFrameworkProperties properties) {
        // SpringDagEngine 的构造函数已更新，这里调用也相应更新
        return new SpringDagEngine(nodeExecutor, properties);
    }
}
