package xyz.vvrf.reactor.dag.spring.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reactor DAG 框架的 Spring Boot 自动配置类 (已重构)。
 * 职责:
 * 1. 启用并绑定 {@link DagFrameworkProperties}。
 * 2. 提供核心的 {@link DagEngineProvider} Bean，它处理引擎的创建。
 * 3. 提供可选的默认 {@link Scheduler} Bean ("dagNodeExecutionScheduler")，现在可由属性配置。
 * 4. 提供可选的默认 {@link Retry} Bean ("dagFrameworkDefaultRetrySpec")，用于框架级别重试。
 * 5. 收集所有的 {@link DagMonitorListener} Bean 到一个列表 Bean ("dagMonitorListeners")。
 * <p>
 * **用户职责:**
 * 1. 实现你的业务逻辑 {@link xyz.vvrf.reactor.dag.core.DagNode}。
 * 2. 使用 {@link xyz.vvrf.reactor.dag.annotation.DagNodeType} 注解实现类，指定 ID 和 contextType。
 * 3. 为每个业务上下文 `MyContext`，定义一个 {@link xyz.vvrf.reactor.dag.registry.NodeRegistry<MyContext>} Bean
 * (通常是 {@link xyz.vvrf.reactor.dag.registry.SpringScanningNodeRegistry<MyContext>})。
 * 4. 在需要执行 DAG 的地方注入 {@link DagEngineProvider} 并调用 {@code getEngine(MyContext.class)}。
 * 5. 使用 {@link xyz.vvrf.reactor.dag.builder.DagDefinitionBuilder} (注入相应的 NodeRegistry) 来构建 DAG 定义。
 * <p>
 * **注意:** 用户不再需要显式定义 {@code NodeExecutor<C>} Bean。
 *
 * @author Refactored
 */
@Configuration
@EnableConfigurationProperties(DagFrameworkProperties.class)
@Slf4j
public class DagFrameworkAutoConfiguration {

    private final ApplicationContext applicationContext;

    public DagFrameworkAutoConfiguration(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        log.info("Reactor DAG 框架自动配置 (DagFrameworkAutoConfiguration) 已加载。");
    }

    /**
     * 提供核心的 DagEngineProvider Bean。
     * 它管理并提供不同上下文类型的 DagEngine 实例，
     * 自动发现所需的 NodeRegistry Bean 并内部创建 NodeExecutor。
     *
     * @param properties            自动注入的框架配置属性。
     * @param frameworkDefaultRetry 注入框架级别的默认重试策略。
     * @return DagEngineProvider 实例。
     */
    @Bean
    @ConditionalOnMissingBean(DagEngineProvider.class)
    public DagEngineProvider dagEngineProvider(DagFrameworkProperties properties, Retry frameworkDefaultRetry) {
        log.info("正在创建 DagEngineProvider Bean...");
        return new DagEngineProvider(properties, frameworkDefaultRetry);
    }

    /**
     * 提供一个默认的 Reactor Scheduler Bean 用于节点执行。
     * 如果已存在名为 "dagNodeExecutionScheduler" 的 Bean，则不创建此默认 Bean。
     * 调度器类型和参数可由 {@link DagFrameworkProperties.SchedulerProps} 配置。
     *
     * @param properties 框架属性。
     * @return 默认的节点执行 Scheduler。
     */
    @Bean(name = "dagNodeExecutionScheduler")
    @ConditionalOnMissingBean(name = "dagNodeExecutionScheduler")
    public Scheduler dagNodeExecutionScheduler(DagFrameworkProperties properties) {
        DagFrameworkProperties.SchedulerProps schedulerProps = properties.getScheduler();
        String namePrefix = schedulerProps.getNamePrefix();

        switch (schedulerProps.getType()) {
            case BOUNDED_ELASTIC:
                DagFrameworkProperties.BoundedElasticProps beProps = schedulerProps.getBoundedElastic();
                log.info("正在创建 'dagNodeExecutionScheduler' (BoundedElastic): prefix={}, cap={}, queue={}, ttl={}s",
                        namePrefix, beProps.getThreadCap(), beProps.getQueuedTaskCap(), beProps.getTtlSeconds());
                return Schedulers.newBoundedElastic(beProps.getThreadCap(), beProps.getQueuedTaskCap(), namePrefix, beProps.getTtlSeconds(), true);
            case PARALLEL:
                DagFrameworkProperties.ParallelProps pProps = schedulerProps.getParallel();
                log.info("正在创建 'dagNodeExecutionScheduler' (Parallel): prefix={}, parallelism={}", namePrefix, pProps.getParallelism());
                return Schedulers.newParallel(namePrefix, pProps.getParallelism(), true);
            case SINGLE:
                log.info("正在创建 'dagNodeExecutionScheduler' (Single): prefix={}", namePrefix);
                return Schedulers.newSingle(namePrefix, true);
            case CUSTOM:
                String customBeanName = schedulerProps.getCustomBeanName();
                if (customBeanName == null || customBeanName.trim().isEmpty()) {
                    log.error("'dag.scheduler.type=CUSTOM' 但 'dag.scheduler.custom-bean-name' 未配置。回退到默认 BoundedElastic。");
                    return Schedulers.newBoundedElastic(
                            schedulerProps.getBoundedElastic().getThreadCap(),
                            schedulerProps.getBoundedElastic().getQueuedTaskCap(),
                            namePrefix + "-fallback",
                            schedulerProps.getBoundedElastic().getTtlSeconds(), true);
                }
                log.info("正在从 Spring 上下文获取自定义 'dagNodeExecutionScheduler' Bean，名称: {}", customBeanName);
                try {
                    return applicationContext.getBean(customBeanName, Scheduler.class);
                } catch (Exception e) {
                    log.error("获取自定义 Scheduler Bean '{}' 失败。回退到默认 BoundedElastic。", customBeanName, e);
                    return Schedulers.newBoundedElastic(
                            schedulerProps.getBoundedElastic().getThreadCap(),
                            schedulerProps.getBoundedElastic().getQueuedTaskCap(),
                            namePrefix + "-fallback-custom-failed",
                            schedulerProps.getBoundedElastic().getTtlSeconds(), true);
                }
            default:
                log.warn("未知的 'dag.scheduler.type': {}. 回退到默认 BoundedElastic。", schedulerProps.getType());
                return Schedulers.newBoundedElastic(
                        schedulerProps.getBoundedElastic().getThreadCap(),
                        schedulerProps.getBoundedElastic().getQueuedTaskCap(),
                        namePrefix + "-default",
                        schedulerProps.getBoundedElastic().getTtlSeconds(), true);
        }
    }

    /**
     * 提供一个框架级别的默认重试策略 Bean。
     * 如果已存在名为 "dagFrameworkDefaultRetrySpec" 的 Bean，则不创建此默认 Bean。
     * 重试策略由 {@link DagFrameworkProperties.RetryProps} 配置。
     *
     * @param properties 框架属性。
     * @return 框架默认的 Retry 规范。
     */
    @Bean(name = "dagFrameworkDefaultRetrySpec")
    @ConditionalOnMissingBean(name = "dagFrameworkDefaultRetrySpec")
    public Retry dagFrameworkDefaultRetrySpec(DagFrameworkProperties properties) {
        DagFrameworkProperties.RetryProps retryProps = properties.getRetry();
        log.info("正在创建框架默认重试策略 'dagFrameworkDefaultRetrySpec'，配置: {}", retryProps);

        if (!retryProps.isEnabled() || retryProps.getMaxAttempts() <= 1) {
            log.info("框架默认重试已禁用或 maxAttempts <= 1。配置为不重试。");
            return Retry.max(0); // 不重试
        }

        // Reactor 的 retry 操作符的第一个参数是 "number of retries"
        // 所以如果 maxAttempts 是总尝试次数，那么 numberOfRetries = maxAttempts - 1
        long numberOfRetries = retryProps.getMaxAttempts() - 1;
        if (numberOfRetries < 0) numberOfRetries = 0; // 防御

        Duration firstBackoff = retryProps.getFirstBackoff();
        if (firstBackoff == null || firstBackoff.isNegative() || firstBackoff.isZero()) {
            log.warn("Retry firstBackoff 配置无效 ({}). 将使用默认值 100ms.", firstBackoff);
            firstBackoff = Duration.ofMillis(100);
        }


        if (retryProps.isUseExponentialBackoff()) {
            log.debug("配置指数退避重试: retries={}, minBackoff={}, maxBackoff={}, jitter={}",
                    numberOfRetries, firstBackoff, retryProps.getMaxBackoff(), retryProps.getJitterFactor());
            RetryBackoffSpec backoffSpec = Retry.backoff(numberOfRetries, firstBackoff);

            Duration maxBackoff = retryProps.getMaxBackoff();
            if (maxBackoff != null && !maxBackoff.isNegative() && !maxBackoff.isZero()) {
                backoffSpec = backoffSpec.maxBackoff(maxBackoff);
            }

            Double jitterFactor = retryProps.getJitterFactor();
            if (jitterFactor != null) {
                if (jitterFactor < 0.0 || jitterFactor > 1.0) {
                    log.warn("Retry jitterFactor ({}) 超出范围 [0.0, 1.0]。将忽略此配置。", jitterFactor);
                } else {
                    backoffSpec = backoffSpec.jitter(jitterFactor);
                }
            }
            return backoffSpec;
        } else {
            log.debug("配置固定延迟重试: retries={}, delay={}", numberOfRetries, firstBackoff);
            return Retry.fixedDelay(numberOfRetries, firstBackoff);
        }
    }


    /**
     * 收集在应用上下文中定义的所有 DagMonitorListener Bean。
     * 将它们作为名为 "dagMonitorListeners" 的不可变列表 Bean 提供。
     * 此列表由 DagEngineProvider 内部创建的 StandardNodeExecutor 使用。
     *
     * @param listenersProvider Spring 的 DagMonitorListener Bean 的 ObjectProvider。
     * @return 包含所有找到的 DagMonitorListener Bean 的不可变列表，如果适用则有序。
     */
    @Bean(name = "dagMonitorListeners")
    @ConditionalOnMissingBean(name = "dagMonitorListeners")
    public List<DagMonitorListener> dagMonitorListeners(ObjectProvider<DagMonitorListener> listenersProvider) {
        log.info("正在收集 DagMonitorListener Bean...");
        List<DagMonitorListener> listeners = listenersProvider.orderedStream().collect(Collectors.toList());
        if (listeners.isEmpty()) {
            log.info("在 Spring 上下文中未找到 DagMonitorListener Bean。");
        } else {
            log.info("收集到 {} 个 DagMonitorListener Bean: {}", listeners.size(),
                    listeners.stream().map(l -> l.getClass().getSimpleName()).collect(Collectors.joining(", ")));
        }
        return Collections.unmodifiableList(listeners);
    }
}
