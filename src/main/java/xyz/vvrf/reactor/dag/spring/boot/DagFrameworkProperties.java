package xyz.vvrf.reactor.dag.spring.boot;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated; // 可选：用于属性校验

import javax.validation.constraints.Min;
import java.time.Duration;

/**
 * DAG框架的配置属性类
 * 绑定 'dag' 前缀下的属性。
 * @author ruifeng.wen (modified)
 */
@Getter
@ConfigurationProperties(prefix = "dag")
@Validated // 可选：启用属性校验
public class DagFrameworkProperties {

    private final Node node = new Node();
    private final Engine engine = new Engine();

    @Getter
    public static class Node {
        /**
         * DAG 节点的默认执行超时时间。
         */
        private Duration defaultTimeout = Duration.ofSeconds(30); // 默认值在此处定义

        public void setDefaultTimeout(Duration defaultTimeout) {
            this.defaultTimeout = defaultTimeout;
        }

        // 未来可能的节点特定属性可以放在这里
    }

    @Getter
    public static class Engine {
        /**
         * DAG 执行期间处理节点的并发级别 (flatMap 的并发度)。
         * 默认为可用处理器的数量。
         *
         * 注意：移除了 cacheTtl 属性，因为请求级缓存的生命周期由请求本身控制，TTL 无意义。
         */
        @Min(1) // 可选：确保并发度至少为 1
        private int concurrencyLevel = Math.max(1, Runtime.getRuntime().availableProcessors()); // 默认值在此处定义

        // 移除了 cacheTtl 的 getter 和 setter
        // public Duration getCacheTtl() { ... }
        // public void setCacheTtl(Duration cacheTtl) { ... }

        public void setConcurrencyLevel(int concurrencyLevel) {
            // 可以在 setter 中添加校验逻辑，或者依赖 @Validated
            this.concurrencyLevel = concurrencyLevel;
        }
    }

    @Override
    public String toString() {
        // 更新 toString 方法以反映移除的 cacheTtl
        return "DagFrameworkProperties{" +
                "node={defaultTimeout=" + node.defaultTimeout +
                "}, engine={concurrencyLevel=" + engine.concurrencyLevel +
                "}}";
    }
}
