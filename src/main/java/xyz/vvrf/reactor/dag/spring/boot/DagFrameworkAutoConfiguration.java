package xyz.vvrf.reactor.dag.spring.boot;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import reactor.core.scheduler.Scheduler; // Assuming Scheduler might be needed later, keep import for now
import reactor.core.scheduler.Schedulers; // Import Schedulers if needed for default
import xyz.vvrf.reactor.dag.impl.StandardNodeExecutor;
import xyz.vvrf.reactor.dag.spring.DagFrameworkConfiguration;
import xyz.vvrf.reactor.dag.spring.SpringDagEngine;

import java.time.Duration;

/**
 * Reactor DAG框架的Spring Boot自动配置类
 * 支持通过Spring Boot应用.properties或.yml配置框架参数
 *
 * @author ruifeng.wen (modified)
 */
@Configuration
@EnableConfigurationProperties(DagFrameworkProperties.class)
@Import(DagFrameworkConfiguration.class)
public class DagFrameworkAutoConfiguration {

    /**
     * 当没有现有的NodeExecutor时，创建一个基于属性配置的NodeExecutor
     * 注意：如果 DagFrameworkConfiguration 也定义了此 Bean，此条件可能不生效。
     */
    @Bean
    @ConditionalOnMissingBean
    public StandardNodeExecutor nodeExecutor(DagFrameworkProperties properties) {
        return new StandardNodeExecutor(
                properties.getNode().getDefaultTimeout()
        );
    }

    /**
     * 当没有现有的SpringDagEngine时，创建一个基于属性配置的SpringDagEngine
     */
    @Bean
    @ConditionalOnMissingBean
    public SpringDagEngine springDagEngine(
            StandardNodeExecutor nodeExecutor, // Correctly depends on the executor bean
            DagFrameworkProperties properties) {
        return new SpringDagEngine(nodeExecutor, properties.getEngine().getCacheTtl());
    }

}
