package xyz.vvrf.reactor.dag.spring;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import xyz.vvrf.reactor.dag.impl.StandardNodeExecutor;

import java.time.Duration;

/**
 * Reactor DAG框架的Spring配置类
 * 提供DAG引擎所需的组件Bean
 *
 * @author ruifeng.wen
 */
@Configuration
@ComponentScan(basePackages = "xyz.vvrf.reactor.dag.spring")
@PropertySource(value = "classpath:dag-framework-default.properties", ignoreResourceNotFound = true)
public class DagFrameworkConfiguration {

    /**
     * 创建标准节点执行器
     *
     * @param defaultNodeTimeout 默认节点执行超时时间
     * @param dependencyStreamTimeout 依赖流等待超时时间
     * @return 标准节点执行器实例
     */
    @Bean
    public StandardNodeExecutor nodeExecutor(
            @Value("${dag.node.default.timeout:30s}") Duration defaultNodeTimeout,
            @Value("${dag.dependency.stream.timeout:300s}") Duration dependencyStreamTimeout) {
        return new StandardNodeExecutor(defaultNodeTimeout, dependencyStreamTimeout);
    }

    /**
     * 创建Spring集成的DAG执行引擎
     *
     * @param nodeExecutor 节点执行器
     * @param cacheTtl 缓存生存时间
     * @return Spring集成的DAG执行引擎
     */
    @Bean
    @ConditionalOnMissingBean
    public SpringDagEngine springDagEngine(
            StandardNodeExecutor nodeExecutor,
            @Value("${dag.engine.cache.ttl:5m}") Duration cacheTtl) {
        return new SpringDagEngine(nodeExecutor, cacheTtl);
    }
}