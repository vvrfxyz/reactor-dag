package xyz.vvrf.reactor.dag.spring.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider; // 使用 ObjectProvider 获取 Bean
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.ResolvableType; // 用于泛型类型解析
import org.springframework.lang.NonNull;
import xyz.vvrf.reactor.dag.execution.DagEngine;
import xyz.vvrf.reactor.dag.execution.NodeExecutor;
import xyz.vvrf.reactor.dag.execution.StandardDagEngine;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DAG 引擎提供者 (重构后)。
 * 负责根据上下文类型查找对应的 {@link NodeRegistry<C>} 和 {@link NodeExecutor<C>} Bean，
 * 并按需创建和缓存 {@link DagEngine<C>} 实例。
 * 它依赖 Spring Boot 自动配置来注入 ApplicationContext 和配置属性。
 * 简化了配置，用户只需定义特定泛型类型的 Registry 和 Executor Bean 即可。
 *
 * @author Refactored (核心重构)
 */
@Slf4j
public class DagEngineProvider implements ApplicationContextAware {

    private ApplicationContext applicationContext;
    private final DagFrameworkProperties properties;

    // 缓存已创建的 DagEngine 实例 <ContextType, DagEngine>
    private final Map<Class<?>, DagEngine<?>> engineCache = new ConcurrentHashMap<>();
    // 注意：不再需要缓存 Registry 和 Executor，因为每次 getEngine 时都会通过 ApplicationContext 查找

    /**
     * 构造函数，由 Spring 调用。
     * @param properties 注入的框架配置属性 (不能为空)。
     */
    public DagEngineProvider(DagFrameworkProperties properties) {
        this.properties = Objects.requireNonNull(properties, "DagFrameworkProperties 不能为空");
        log.info("DagEngineProvider 已初始化，配置: {}", properties);
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        log.info("DagEngineProvider 已接收 ApplicationContext。");
    }

    /**
     * 根据上下文类型获取对应的 DAG 执行引擎实例。
     * 如果缓存中不存在，则尝试查找匹配的、唯一的 NodeRegistry<C> 和 NodeExecutor<C> Bean 来创建引擎。
     *
     * @param contextType 需要的 DAG 上下文类型 Class 对象 (不能为空)。
     * @param <C>         上下文类型。
     * @return 对应上下文类型的 DagEngine<C> 实例。
     * @throws NoSuchBeanDefinitionException 如果找不到 contextType 对应的 NodeRegistry 或 NodeExecutor Bean。
     * @throws NoUniqueBeanDefinitionException 如果找到多个 contextType 对应的 NodeRegistry 或 NodeExecutor Bean。
     * @throws IllegalStateException 如果 ApplicationContext 未被设置。
     */
    @SuppressWarnings("unchecked") // 类型转换是安全的，因为我们基于 Class<C> 查找和创建
    public <C> DagEngine<C> getEngine(Class<C> contextType) {
        Objects.requireNonNull(contextType, "上下文类型不能为空");
        if (this.applicationContext == null) {
            throw new IllegalStateException("ApplicationContext 尚未在 DagEngineProvider 中设置。");
        }

        // 1. 尝试从缓存获取
        DagEngine<?> cachedEngine = engineCache.get(contextType);
        if (cachedEngine != null) {
            log.debug("为上下文类型 '{}' 返回缓存的 DagEngine 实例。", contextType.getSimpleName());
            return (DagEngine<C>) cachedEngine;
        }

        // 2. 缓存未命中，查找依赖并创建
        log.info("上下文类型 '{}' 的 DagEngine 不在缓存中，尝试创建...", contextType.getSimpleName());

        // 3. 查找唯一的 NodeRegistry<C> Bean
        NodeRegistry<C> registry = findUniqueBeanForContext(NodeRegistry.class, contextType);

        // 4. 查找唯一的 NodeExecutor<C> Bean
        NodeExecutor<C> executor = findUniqueBeanForContext(NodeExecutor.class, contextType);

        // 5. 验证 Registry 和 Executor 的上下文类型是否匹配 (双重检查)
        if (!registry.getContextType().equals(contextType) || !executor.getContextType().equals(contextType)) {
            throw new IllegalStateException(String.format(
                    "为上下文 '%s' 找到的 NodeRegistry (实际类型: %s) 或 NodeExecutor (实际类型: %s) 的 getContextType() 返回值不匹配！",
                    contextType.getSimpleName(),
                    registry.getContextType().getSimpleName(),
                    executor.getContextType().getSimpleName()));
        }


        // 6. 创建 StandardDagEngine 实例
        log.debug("为上下文 '{}' 找到唯一的 NodeRegistry ({}) 和 NodeExecutor ({}). 创建 StandardDagEngine...",
                contextType.getSimpleName(), registry.getClass().getSimpleName(), executor.getClass().getSimpleName());
        StandardDagEngine<C> newEngine = new StandardDagEngine<>(
                registry,
                executor,
                properties.getEngine().getConcurrencyLevel() // 使用配置的并发级别
        );

        // 7. 放入缓存 (使用 computeIfAbsent 避免并发问题)
        DagEngine<?> existingEngine = engineCache.putIfAbsent(contextType, newEngine);
        if (existingEngine != null) {
            log.warn("在并发创建 DagEngine 时，上下文 '{}' 的实例已被其他线程创建。返回已存在的实例。", contextType.getSimpleName());
            return (DagEngine<C>) existingEngine; // 返回已存在的实例
        } else {
            log.info("已成功创建并缓存上下文类型 '{}' 的 DagEngine 实例。", contextType.getSimpleName());
            return newEngine; // 返回新创建的实例
        }
    }

    /**
     * 查找 Spring 应用上下文中唯一的、匹配指定基础类型和上下文泛型参数的 Bean。
     *
     * @param baseType    Bean 的基础类型 (例如 NodeRegistry.class)
     * @param contextType 期望的泛型上下文类型
     * @param <T>         Bean 的基础类型
     * @param <C>         上下文类型
     * @return 找到的唯一 Bean 实例
     * @throws NoSuchBeanDefinitionException 如果找不到匹配的 Bean
     * @throws NoUniqueBeanDefinitionException 如果找到多个匹配的 Bean
     */
    private <T, C> T findUniqueBeanForContext(Class<T> baseType, Class<C> contextType) {
        // 构建需要查找的泛型类型: T<C>
        ResolvableType requiredType = ResolvableType.forClassWithGenerics(baseType, contextType);

        // 使用 ObjectProvider 来获取 Bean，它能更好地处理找不到或找到多个的情况
        ObjectProvider<T> beanProvider = applicationContext.getBeanProvider(requiredType);

        // 尝试获取唯一的 Bean，如果找不到或找到多个，会抛出相应的异常
        try {
            // getIfUnique() 在找不到时返回 null，找到多个时抛 NoUniqueBeanDefinitionException
            T uniqueBean = beanProvider.getIfUnique();
            if (uniqueBean != null) {
                log.debug("为类型 {}< {} > 找到了唯一的 Bean: {}", baseType.getSimpleName(), contextType.getSimpleName(), uniqueBean.getClass().getName());
                return uniqueBean;
            } else {
                // getIfUnique 返回 null 意味着没有找到 Bean
                throw new NoSuchBeanDefinitionException(requiredType,
                        String.format("在 Spring 上下文中未找到类型为 %s<%s> 的 Bean 定义。",
                                baseType.getSimpleName(), contextType.getSimpleName()));
            }
        } catch (NoUniqueBeanDefinitionException e) {
            log.error("为类型 {}<{}> 找到了多个 Bean 定义: {}", baseType.getSimpleName(), contextType.getSimpleName(), String.join(", ", e.getBeanNamesFound()));
            throw e;
        }
    }


    /**
     * 清除内部缓存。主要用于测试或需要重新加载配置的场景。
     */
    public void clearCache() {
        log.warn("正在清除 DagEngineProvider 的缓存...");
        engineCache.clear();
        log.warn("DagEngineProvider 缓存已清除。");
    }
}
