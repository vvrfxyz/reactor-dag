package xyz.vvrf.reactor.dag.spring.boot;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

/**
 * DAG框架的配置属性类
 */
@ConfigurationProperties(prefix = "dag")
class DagFrameworkProperties {

    private final Node node = new Node();
    private final Engine engine = new Engine();

    public Node getNode() {
        return node;
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
