package xyz.vvrf.reactor.dag.spring.boot;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated; // 可选：用于属性校验

import javax.validation.constraints.Min;
import java.time.Duration;

/**
 * DAG框架的配置属性类
 * 绑定 'dag' 前缀下的属性。
 * @author ruifeng.wen
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
         * 请求级别节点结果缓存的生存时间 (TTL)。
         * 设置为 PT0S (Duration.ZERO) 可有效禁用基于时间的缓存。
         */
        private Duration cacheTtl = Duration.ofMinutes(5); // 默认值在此处定义

        /**
         * DAG 执行期间处理节点的并发级别 (flatMap 的并发度)。
         * 默认为可用处理器的数量。
         */
        @Min(1) // 可选：确保并发度至少为 1
        private int concurrencyLevel = Math.max(1, Runtime.getRuntime().availableProcessors()); // 默认值在此处定义

        public void setCacheTtl(Duration cacheTtl) {
            this.cacheTtl = cacheTtl;
        }

        public void setConcurrencyLevel(int concurrencyLevel) {
            // 可以在 setter 中添加校验逻辑，或者依赖 @Validated
            this.concurrencyLevel = concurrencyLevel;
        }
    }

    // 可以添加 toString() 方法，方便调试和日志记录属性
    @Override
    public String toString() {
        return "DagFrameworkProperties{" +
                "node={defaultTimeout=" + node.defaultTimeout +
                "}, engine={cacheTtl=" + engine.cacheTtl +
                ", concurrencyLevel=" + engine.concurrencyLevel +
                "}}";
    }
}
