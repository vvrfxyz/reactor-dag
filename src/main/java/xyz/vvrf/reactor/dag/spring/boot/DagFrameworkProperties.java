// file: DagFrameworkProperties.java
package xyz.vvrf.reactor.dag.spring.boot;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import java.time.Duration;

/**
 * DAG框架的配置属性类 (更新后)
 * 绑定 'dag' 前缀下的属性。
 * @author ruifeng.wen (modified)
 */
@Getter
@ConfigurationProperties(prefix = "dag")
@Validated
public class DagFrameworkProperties {

    private final Node node = new Node();
    private final Engine engine = new Engine();

    @Getter
    public static class Node {
        /**
         * DAG 节点的默认执行超时时间。
         */
        private Duration defaultTimeout = Duration.ofSeconds(30);

        public void setDefaultTimeout(Duration defaultTimeout) {
            // 可选：添加校验，如非负
            this.defaultTimeout = defaultTimeout;
        }
    }

    @Getter
    public static class Engine {
        /**
         * DAG 执行期间处理节点的并发级别。
         * 注意：这可能不再直接映射到 flatMap 的并发度，取决于引擎实现，但仍可作为引擎级别的配置。
         * 默认为可用处理器的数量。
         */
        @Min(1)
        private int concurrencyLevel = Math.max(1, Runtime.getRuntime().availableProcessors());

        // cacheTtl 属性已移除

        public void setConcurrencyLevel(int concurrencyLevel) {
            this.concurrencyLevel = concurrencyLevel;
        }
    }

    @Override
    public String toString() {
        // 更新 toString 方法
        return "DagFrameworkProperties{" +
                "node={defaultTimeout=" + node.defaultTimeout +
                "}, engine={concurrencyLevel=" + engine.concurrencyLevel +
                "}}";
    }
}
