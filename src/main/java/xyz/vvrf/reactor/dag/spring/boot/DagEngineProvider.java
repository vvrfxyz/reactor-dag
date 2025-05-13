package xyz.vvrf.reactor.dag.spring.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.ResolvableType;
import org.springframework.lang.NonNull;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.execution.DagEngine;
import xyz.vvrf.reactor.dag.execution.StandardDagEngine;
import xyz.vvrf.reactor.dag.execution.StandardNodeExecutor;
import xyz.vvrf.reactor.dag.monitor.DagMonitorListener;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * DAG 引擎提供者 (已重构)。
 * 负责根据上下文类型查找相应的 {@link NodeRegistry<C>} Bean，
 * 并按需创建和缓存 {@link DagEngine<C>} 实例。
 * 它现在内部创建 {@link StandardNodeExecutor}，使用自动配置的
 * 默认组件 (Scheduler, Listeners, Properties, DefaultRetry)，通过名称查找这些默认组件。
 *
 * @author Refactored
 */
@Slf4j
public class DagEngineProvider implements ApplicationContextAware {

    // 自动配置中定义的 Bean 名称
    private static final String DEFAULT_SCHEDULER_BEAN_NAME = "dagNodeExecutionScheduler";
    private static final String DEFAULT_LISTENERS_BEAN_NAME = "dagMonitorListeners";
    // 框架默认重试Bean名称 (如果通过Bean提供) - 当前直接从AutoConfig注入
    // private static final String FRAMEWORK_DEFAULT_RETRY_BEAN_NAME = "dagFrameworkDefaultRetrySpec";


    private ApplicationContext applicationContext;
    private final DagFrameworkProperties properties;
    private final Retry frameworkDefaultRetrySpec;

    // 缓存已创建的 DagEngine 实例 <ContextType, DagEngine>
    private final Map<Class<?>, DagEngine<?>> engineCache = new ConcurrentHashMap<>();

    /**
     * 由 Spring 调用的构造函数。
     *
     * @param properties                注入的框架配置属性 (非空)。
     * @param frameworkDefaultRetrySpec 注入的框架默认重试策略 (非空, 可以是 Retry.max(0))。
     */
    public DagEngineProvider(DagFrameworkProperties properties, Retry frameworkDefaultRetrySpec) {
        this.properties = Objects.requireNonNull(properties, "DagFrameworkProperties 不能为空");
        this.frameworkDefaultRetrySpec = Objects.requireNonNull(frameworkDefaultRetrySpec, "框架默认重试策略不能为空");
        log.info("DagEngineProvider 已初始化，配置: {}, 框架默认重试: {}", properties, this.frameworkDefaultRetrySpec.getClass().getSimpleName());
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        log.info("DagEngineProvider 已接收 ApplicationContext。");
    }

    /**
     * 获取指定上下文类型的 DAG 执行引擎实例。
     * 如果未缓存，它会查找唯一的匹配 NodeRegistry<C> Bean，并
     * 内部创建 StandardNodeExecutor<C> 和 StandardDagEngine<C>。
     * 它会尝试按名称查找自动配置提供的默认 Scheduler 和 Listeners。
     *
     * @param contextType 需要的 DAG 上下文类型 Class 对象 (非空)。
     * @param <C>         上下文类型。
     * @return 上下文类型的 DagEngine<C> 实例。
     * @throws NoSuchBeanDefinitionException   如果找不到所需的 NodeRegistry<C> Bean，
     *                                         或者按名称查找时找不到默认的调度器/监听器 Bean。
     * @throws NoUniqueBeanDefinitionException 如果找到多个 NodeRegistry<C> Bean。
     * @throws IllegalStateException           如果 ApplicationContext 尚未设置。
     */
    @SuppressWarnings("unchecked") // 类型转换是安全的，因为基于 Class<C> 查找和创建
    public <C> DagEngine<C> getEngine(Class<C> contextType) {
        Objects.requireNonNull(contextType, "上下文类型不能为空");
        if (this.applicationContext == null) {
            throw new IllegalStateException("ApplicationContext 尚未在 DagEngineProvider 中设置。");
        }

        // 1. 首先尝试缓存
        DagEngine<?> cachedEngine = engineCache.get(contextType);
        if (cachedEngine != null) {
            log.debug("为上下文类型 '{}' 返回缓存的 DagEngine 实例。", contextType.getSimpleName());
            return (DagEngine<C>) cachedEngine;
        }

        // 2. 缓存未命中，查找依赖并创建引擎
        log.info("上下文类型 '{}' 的 DagEngine 不在缓存中，尝试创建...", contextType.getSimpleName());

        // 3. 查找唯一的 NodeRegistry<C> Bean
        NodeRegistry<C> registry = findUniqueBeanForContext(NodeRegistry.class, contextType);
        log.debug("为上下文 '{}' 找到了唯一的 NodeRegistry: {}", contextType.getSimpleName(), registry.getClass().getName());

        // 4. 按名称查找默认的 Scheduler Bean (StandardNodeExecutor 需要)
        Scheduler scheduler;
        try {
            scheduler = applicationContext.getBean(DEFAULT_SCHEDULER_BEAN_NAME, Scheduler.class);
            log.debug("找到了名为 '{}' 的默认 Scheduler Bean: {}", DEFAULT_SCHEDULER_BEAN_NAME, scheduler.getClass().getName());
        } catch (NoSuchBeanDefinitionException e) {
            log.warn("按名称 '{}' 找不到默认的 Scheduler Bean。回退到 Schedulers.immediate()。请检查 DagFrameworkAutoConfiguration 是否已启用或被覆盖。", DEFAULT_SCHEDULER_BEAN_NAME);
            scheduler = Schedulers.immediate(); // 提供一个基础的回退
        }

        // 5. 按名称查找 DagMonitorListener Bean 列表 (StandardNodeExecutor 需要)
        List<DagMonitorListener> listeners;
        try {
            Object listenersBean = applicationContext.getBean(DEFAULT_LISTENERS_BEAN_NAME);
            if (listenersBean instanceof List) {
                listeners = ((List<?>) listenersBean).stream()
                        .filter(DagMonitorListener.class::isInstance)
                        .map(DagMonitorListener.class::cast)
                        .collect(Collectors.toList());
                log.debug("找到了名为 '{}' 的默认 List<DagMonitorListener> Bean，包含 {} 个监听器。", DEFAULT_LISTENERS_BEAN_NAME, listeners.size());
            } else {
                log.error("名为 '{}' 的 Bean 不是 List 类型。期望是 List<DagMonitorListener>。将使用空列表。", DEFAULT_LISTENERS_BEAN_NAME);
                listeners = Collections.emptyList();
            }
        } catch (NoSuchBeanDefinitionException e) {
            log.warn("按名称 '{}' 找不到默认的 List<DagMonitorListener> Bean。将使用空列表。请检查 DagFrameworkAutoConfiguration 是否已启用或被覆盖。", DEFAULT_LISTENERS_BEAN_NAME);
            listeners = Collections.emptyList();
        }


        // 6. 在内部创建 StandardNodeExecutor
        log.debug("为上下文 '{}' 创建内部 StandardNodeExecutor...", contextType.getSimpleName());
        StandardNodeExecutor<C> executor = new StandardNodeExecutor<>(
                registry,
                properties.getNode().getDefaultTimeout(),
                scheduler,
                listeners,
                this.frameworkDefaultRetrySpec
        );

        // 7. 创建 StandardDagEngine 实例
        log.debug("为上下文 '{}' 创建 StandardDagEngine...", contextType.getSimpleName());
        StandardDagEngine<C> newEngine = new StandardDagEngine<>(
                registry,
                executor,
                properties.getEngine().getConcurrencyLevel(),
                listeners // StandardDagEngine 也接收监听器列表
        );

        // 8. 放入缓存 (使用 computeIfAbsent 保证线程安全)
        DagEngine<?> existingEngine = engineCache.putIfAbsent(contextType, newEngine);
        if (existingEngine != null) {
            log.warn("上下文 '{}' 的 DagEngine 被并发创建。返回已存在的实例。", contextType.getSimpleName());
            return (DagEngine<C>) existingEngine;
        } else {
            log.info("已成功创建并缓存上下文类型 '{}' 的 DagEngine 实例。", contextType.getSimpleName());
            return newEngine;
        }
    }

    /**
     * 查找匹配基础类型和上下文泛型参数的唯一 Spring Bean。
     * (辅助方法，未改变)
     */
    private <T, C> T findUniqueBeanForContext(Class<T> baseType, Class<C> contextType) {
        ResolvableType requiredType = ResolvableType.forClassWithGenerics(baseType, contextType);
        ObjectProvider<T> beanProvider = applicationContext.getBeanProvider(requiredType);

        try {
            T uniqueBean = beanProvider.getIfUnique();
            if (uniqueBean != null) {
                log.trace("为类型 {}<{}> 找到了唯一的 Bean: {}", baseType.getSimpleName(), contextType.getSimpleName(), uniqueBean.getClass().getName());
                return uniqueBean;
            } else {
                // ObjectProvider.getIfUnique() 返回 null 如果没有找到或找到多个。我们需要区分这两种情况。
                // Spring 5.3+ getIfAvailable() + getIfUnique() or check beans by names
                String[] beanNames = applicationContext.getBeanNamesForType(requiredType);
                if (beanNames.length == 0) {
                    throw new NoSuchBeanDefinitionException(requiredType,
                            String.format("在 Spring 上下文中找不到类型为 %s<%s> 的 Bean 定义。",
                                    baseType.getSimpleName(), contextType.getSimpleName()));
                } else if (beanNames.length > 1) {
                    throw new NoUniqueBeanDefinitionException(requiredType, beanNames);
                }
                // Should not happen if beanNames.length == 1 and uniqueBean was null, but as a fallback:
                return applicationContext.getBean(beanNames[0], baseType); // This might not respect generics fully
            }
        } catch (NoUniqueBeanDefinitionException e) {
            String beanNamesStr = String.join(", ", applicationContext.getBeanNamesForType(requiredType));
            log.error("为类型 {}<{}> 找到了多个 Bean 定义: [{}]. 期望只有一个。",
                    baseType.getSimpleName(), contextType.getSimpleName(), beanNamesStr);
            throw new NoUniqueBeanDefinitionException(requiredType, String.valueOf(e.getNumberOfBeansFound()),
                    String.format("期望 %s<%s> 只有一个 Bean，但找到了 %d 个: [%s]",
                            baseType.getSimpleName(), contextType.getSimpleName(), e.getNumberOfBeansFound(), beanNamesStr));
        }
    }


    /**
     * 清除内部引擎缓存。用于测试或重载场景。
     */
    public void clearCache() {
        log.warn("正在清除 DagEngineProvider 缓存...");
        engineCache.clear();
        log.warn("DagEngineProvider 缓存已清除。");
    }
}
