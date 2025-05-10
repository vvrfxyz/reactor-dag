package xyz.vvrf.reactor.dag.spring.boot;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;
import reactor.core.scheduler.Schedulers;

import javax.validation.Valid;
import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import java.time.Duration;

/**
 * DAG框架的配置属性类 (更新后)
 * 绑定 'dag' 前缀下的属性。
 *
 * @author ruifeng.wen
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "dag")
@Validated
public class DagFrameworkProperties {

    @Valid
    private final Node node = new Node();
    @Valid
    private final Engine engine = new Engine();
    @Valid
    private final SchedulerProps scheduler = new SchedulerProps();
    @Valid
    private final RetryProps retry = new RetryProps();

    @Getter
    @Setter
    public static class Node {
        /**
         * DAG 节点的默认执行超时时间。
         */
        private Duration defaultTimeout = Duration.ofSeconds(30);
    }

    @Getter
    @Setter
    public static class Engine {
        /**
         * DAG 执行期间处理节点的并发级别。
         * 注意：这可能不再直接映射到 flatMap 的并发度，取决于引擎实现，但仍可作为引擎级别的配置。
         * 默认为可用处理器的数量。
         */
        @Min(1)
        private int concurrencyLevel = Math.max(1, Runtime.getRuntime().availableProcessors());
    }

    @Getter
    @Setter
    public static class SchedulerProps {
        /**
         * 调度器类型。
         */
        private SchedulerType type = SchedulerType.BOUNDED_ELASTIC;

        /**
         * 调度器名称前缀。
         */
        private String namePrefix = "dag-exec";

        /**
         * BoundedElastic 调度器特定配置。
         */
        @Valid
        private final BoundedElasticProps boundedElastic = new BoundedElasticProps();

        /**
         * Parallel 调度器特定配置。
         */
        @Valid
        private final ParallelProps parallel = new ParallelProps();

        /**
         * Single 调度器特定配置。
         */
        @Valid
        private final SingleProps single = new SingleProps();

        /**
         * 当 type 为 CUSTOM 时，自定义 Scheduler Bean 的名称。
         */
        private String customBeanName;
    }

    public enum SchedulerType {
        BOUNDED_ELASTIC, PARALLEL, SINGLE, CUSTOM
    }

    @Getter
    @Setter
    public static class BoundedElasticProps {
        @Min(1)
        private int threadCap = Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE;
        @Min(1)
        private int queuedTaskCap = Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE;
        @Min(0) // TTL can be 0 for non-daemon threads or short-lived tasks
        private int ttlSeconds = 60;
    }

    @Getter
    @Setter
    public static class ParallelProps {
        @Min(1)
        private int parallelism = Runtime.getRuntime().availableProcessors();
    }

    @Getter
    @Setter
    public static class SingleProps {
        // Currently no specific properties for Schedulers.newSingle() other than name
    }


    @Getter
    @Setter
    public static class RetryProps {
        /**
         * 是否启用框架级别的默认重试策略。
         */
        private boolean enabled = true;

        /**
         * 最大总尝试次数 (1 表示不重试)。
         */
        @Min(1)
        private long maxAttempts = 3;

        /**
         * 首次/固定退避延迟时间。
         */
        private Duration firstBackoff = Duration.ofMillis(100);

        /**
         * 是否使用指数退避。如果为 false，则使用 firstBackoff 作为固定延迟。
         */
        private boolean useExponentialBackoff = false;

        /**
         * (仅用于指数退避) 最大退避延迟时间。
         */
        private Duration maxBackoff = Duration.ofSeconds(10);

        /**
         * (仅用于指数退避) 抖动因子 (0.0 到 1.0)。建议 0.0 到 0.8。
         */
        @DecimalMin("0.0")
        @DecimalMax("1.0")
        private Double jitterFactor = 0.5; // Default to 0.5 as in Retry.backoff()
    }


    @Override
    public String toString() {
        return "DagFrameworkProperties{" +
                "node={defaultTimeout=" + node.defaultTimeout +
                "}, engine={concurrencyLevel=" + engine.concurrencyLevel +
                "}, scheduler={type=" + scheduler.type +
                ", namePrefix='" + scheduler.namePrefix + '\'' +
                ", boundedElastic={threadCap=" + scheduler.boundedElastic.threadCap +
                ", queuedTaskCap=" + scheduler.boundedElastic.queuedTaskCap +
                ", ttlSeconds=" + scheduler.boundedElastic.ttlSeconds +
                "}, parallel={parallelism=" + scheduler.parallel.parallelism +
                "}, customBeanName='" + scheduler.customBeanName + '\'' +
                "}, retry={enabled=" + retry.enabled +
                ", maxAttempts=" + retry.maxAttempts +
                ", firstBackoff=" + retry.firstBackoff +
                ", useExponentialBackoff=" + retry.useExponentialBackoff +
                ", maxBackoff=" + retry.maxBackoff +
                ", jitterFactor=" + retry.jitterFactor +
                "}}";
    }
}
