package xyz.vvrf.reactor.dag.spring.boot;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import xyz.vvrf.reactor.dag.impl.StandardNodeExecutor;
import xyz.vvrf.reactor.dag.spring.DagFrameworkConfiguration;
import xyz.vvrf.reactor.dag.spring.SpringDagEngine;

import java.time.Duration;

/**
 * Reactor DAG框架的Spring Boot自动配置类
 * 支持通过Spring Boot应用.properties或.yml配置框架参数
 *
 * @author ruifeng.wen
 */
@Configuration
@EnableConfigurationProperties(DagFrameworkProperties.class)
@Import(DagFrameworkConfiguration.class)
public class DagFrameworkAutoConfiguration {

    /**
     * 当没有现有的NodeExecutor时，创建一个基于属性配置的NodeExecutor
     */
    @Bean
    @ConditionalOnMissingBean
    public StandardNodeExecutor nodeExecutor(DagFrameworkProperties properties) {
        return new StandardNodeExecutor(
                properties.getNode().getDefaultTimeout(),
                properties.getDependency().getStreamTimeout());
    }

    /**
     * 当没有现有的SpringDagEngine时，创建一个基于属性配置的SpringDagEngine
     */
    @Bean
    @ConditionalOnMissingBean
    public SpringDagEngine springDagEngine(
            StandardNodeExecutor nodeExecutor,
            DagFrameworkProperties properties) {
        return new SpringDagEngine(nodeExecutor, properties.getEngine().getCacheTtl());
    }
}

/**
 * DAG框架的配置属性类
 */
@ConfigurationProperties(prefix = "dag")
class DagFrameworkProperties {

    private final Node node = new Node();
    private final Dependency dependency = new Dependency();
    private final Engine engine = new Engine();

    public Node getNode() {
        return node;
    }

    public Dependency getDependency() {
        return dependency;
    }

    public Engine getEngine() {
        return engine;
    }

    public static class Node {
        /**
         * 默认节点执行超时时间
         */
        private Duration defaultTimeout = Duration.ofSeconds(30);

        public Duration getDefaultTimeout() {
            return defaultTimeout;
        }

        public void setDefaultTimeout(Duration defaultTimeout) {
            this.defaultTimeout = defaultTimeout;
        }
    }

    public static class Dependency {
        /**
         * 依赖流等待超时时间
         */
        private Duration streamTimeout = Duration.ofMinutes(3);

        public Duration getStreamTimeout() {
            return streamTimeout;
        }

        public void setStreamTimeout(Duration streamTimeout) {
            this.streamTimeout = streamTimeout;
        }
    }

    public static class Engine {
        /**
         * 缓存生存时间
         */
        private Duration cacheTtl = Duration.ofMinutes(5);

        public Duration getCacheTtl() {
            return cacheTtl;
        }

        public void setCacheTtl(Duration cacheTtl) {
            this.cacheTtl = cacheTtl;
        }
    }
}