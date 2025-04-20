package xyz.vvrf.reactor.dag.spring.boot;

import org.springframework.beans.factory.ObjectProvider; // 用于可选注入
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
// 移除: import org.springframework.context.annotation.Import;
// 移除: import xyz.vvrf.reactor.dag.spring.DagFrameworkConfiguration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.impl.StandardNodeExecutor;
import xyz.vvrf.reactor.dag.spring.SpringDagEngine; // 保留此导入

/**
 * Reactor DAG框架的Spring Boot自动配置类。
 * 基于 DagFrameworkProperties 配置框架。
 * 如果用户未定义，则提供默认的 Bean。
 *
 * @author ruifeng.wen (modified)
 */
@Configuration
@EnableConfigurationProperties(DagFrameworkProperties.class) // 启用属性绑定
public class DagFrameworkAutoConfiguration {

    /**
     * 如果上下文中不存在 StandardNodeExecutor Bean，则提供一个默认的实例。
     * 使用 DagFrameworkProperties 进行配置。
     * 允许用户自定义用于节点执行的 Scheduler Bean。
     *
     * @param properties 绑定的配置属性。
     * @param nodeExecutionSchedulerProvider 用户可选提供的 Scheduler Bean。
     *                               如果不存在，则默认为 Schedulers.boundedElastic()。
     * @return StandardNodeExecutor 实例。
     */
    @Bean
    @ConditionalOnMissingBean
    public StandardNodeExecutor standardNodeExecutor(
            DagFrameworkProperties properties,
            ObjectProvider<Scheduler> nodeExecutionSchedulerProvider
    ) {
        Scheduler scheduler = nodeExecutionSchedulerProvider.getIfAvailable(Schedulers::boundedElastic);
        return new StandardNodeExecutor(
                properties.getNode().getDefaultTimeout(),
                scheduler
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
        return new SpringDagEngine(nodeExecutor, properties);
    }
}
