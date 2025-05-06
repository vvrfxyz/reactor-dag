// file: spring/boot/DagEngineProvider.java
package xyz.vvrf.reactor.dag.spring.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.lang.NonNull;
import xyz.vvrf.reactor.dag.execution.DagEngine;
import xyz.vvrf.reactor.dag.execution.NodeExecutor;
import xyz.vvrf.reactor.dag.execution.StandardDagEngine;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * DAG 引擎提供者。
 * 负责根据上下文类型查找对应的 {@link NodeRegistry} 和 {@link NodeExecutor} Bean，
 * 并按需创建和缓存 {@link DagEngine} 实例。
 * 由 Spring Boot 自动配置。
 *
 * @author Refactored
 */
@Slf4j
public class DagEngineProvider implements ApplicationContextAware {

    private ApplicationContext applicationContext;
    private final DagFrameworkProperties properties;

    // 缓存已创建的 DagEngine 实例 <ContextType, DagEngine>
    private final Map<Class<?>, DagEngine<?>> engineCache = new ConcurrentHashMap<>();
    // 缓存已找到的 NodeRegistry 实例 <ContextType, NodeRegistry>
    private final Map<Class<?>, NodeRegistry<?>> registryCache = new ConcurrentHashMap<>();
    // 缓存已找到的 NodeExecutor 实例 <ContextType, NodeExecutor>
    private final Map<Class<?>, NodeExecutor<?>> executorCache = new ConcurrentHashMap<>();

    /**
     * 构造函数，由 Spring 调用。
     * @param properties 注入的框架配置属性。
     */
    public DagEngineProvider(DagFrameworkProperties properties) {
        this.properties = Objects.requireNonNull(properties, "DagFrameworkProperties cannot be null");
        log.info("DagEngineProvider initialized with properties: {}", properties);
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        // 应用上下文设置后，可以预先查找并缓存 Registry 和 Executor (可选)
        // 或者在 getEngine 时懒加载查找
        // 这里选择懒加载查找，更灵活
        log.info("DagEngineProvider received ApplicationContext.");
    }

    /**
     * 根据上下文类型获取对应的 DAG 执行引擎实例。
     * 如果缓存中不存在，则尝试查找匹配的 NodeRegistry 和 NodeExecutor Bean 来创建引擎。
     *
     * @param contextType 需要的 DAG 上下文类型 Class 对象。
     * @param <C>         上下文类型。
     * @return 对应上下文类型的 DagEngine 实例。
     * @throws IllegalStateException 如果无法找到 contextType 对应的 NodeRegistry 或 NodeExecutor Bean。
     */
    @SuppressWarnings("unchecked") // 类型转换是安全的，因为我们基于 Class<C> 查找和创建
    public <C> DagEngine<C> getEngine(Class<C> contextType) {
        Objects.requireNonNull(contextType, "Context type cannot be null");

        // 尝试从缓存获取
        DagEngine<?> cachedEngine = engineCache.get(contextType);
        if (cachedEngine != null) {
            log.debug("Returning cached DagEngine for context type: {}", contextType.getSimpleName());
            return (DagEngine<C>) cachedEngine;
        }

        // 缓存未命中，查找依赖并创建
        log.info("DagEngine for context type '{}' not found in cache. Attempting to create...", contextType.getSimpleName());

        // 查找 NodeRegistry<C>
        NodeRegistry<C> registry = findRegistryForContext(contextType);

        // 查找 NodeExecutor<C>
        NodeExecutor<C> executor = findExecutorForContext(contextType);

        // 创建 StandardDagEngine 实例
        log.debug("Found NodeRegistry ({}) and NodeExecutor ({}) for context '{}'. Creating StandardDagEngine...",
                registry.getClass().getSimpleName(), executor.getClass().getSimpleName(), contextType.getSimpleName());
        StandardDagEngine<C> newEngine = new StandardDagEngine<>(
                registry,
                executor,
                properties.getEngine().getConcurrencyLevel() // 使用配置的并发级别
        );

        // 放入缓存
        engineCache.put(contextType, newEngine);
        log.info("Successfully created and cached DagEngine for context type: {}", contextType.getSimpleName());

        return newEngine;
    }

    /**
     * 查找与指定上下文类型匹配的 NodeRegistry Bean。
     */
    @SuppressWarnings("unchecked")
    private <C> NodeRegistry<C> findRegistryForContext(Class<C> contextType) {
        // 尝试从缓存获取
        NodeRegistry<?> cachedRegistry = registryCache.get(contextType);
        if (cachedRegistry != null) {
            return (NodeRegistry<C>) cachedRegistry;
        }

        // 从 Spring Context 查找所有 NodeRegistry Bean
        Map<String, NodeRegistry> registries = applicationContext.getBeansOfType(NodeRegistry.class);
        if (registries.isEmpty()) {
            throw new IllegalStateException("No NodeRegistry beans found in the application context. " +
                    "Please define at least one NodeRegistry bean for your context type(s).");
        }

        // 过滤出与 contextType 匹配的 Registry
        NodeRegistry<C> foundRegistry = null;
        for (NodeRegistry<?> registry : registries.values()) {
            if (registry.getContextType().equals(contextType)) {
                if (foundRegistry != null) {
                    // 找到多个匹配的 Registry，这是配置错误
                    throw new IllegalStateException(
                            String.format("Multiple NodeRegistry beans found for context type '%s'. Please ensure only one bean per context type.",
                                    contextType.getSimpleName()));
                }
                foundRegistry = (NodeRegistry<C>) registry;
            }
            // 同时缓存所有找到的 registry
            registryCache.putIfAbsent(registry.getContextType(), registry);
        }

        if (foundRegistry == null) {
            // 没有找到匹配的 Registry
            String availableContexts = registries.values().stream()
                    .map(r -> r.getContextType().getSimpleName())
                    .collect(Collectors.joining(", "));
            throw new NoSuchBeanDefinitionException(NodeRegistry.class,
                    String.format("No NodeRegistry bean found specifically for context type '%s'. Available context types with registries: [%s]",
                            contextType.getSimpleName(), availableContexts));
        }

        log.debug("Found NodeRegistry bean for context type '{}': {}", contextType.getSimpleName(), foundRegistry.getClass().getName());
        return foundRegistry;
    }

    /**
     * 查找与指定上下文类型匹配的 NodeExecutor Bean。
     * 注意：NodeExecutor 本身不是泛型的，但我们期望用户定义的 Bean 是针对特定上下文配置的，
     * 并且通常会依赖于对应上下文的 NodeRegistry。我们需要一种方式来关联 Executor 和 Context。
     *
     * 策略：查找所有 NodeExecutor Bean，然后找到那个依赖于我们刚找到的 NodeRegistry<C> 的 Executor。
     * 这依赖于 Spring 的依赖注入关系。这是一个间接的关联方式。
     *
     * 更好的策略（如果可行）：让 NodeExecutor<C> 接口也包含 getContextType()。但 NodeExecutor 的核心方法
     * executeNode 签名已经是 <C>，使其自身也泛型 <C> 会更复杂。
     *
     * 折中策略：查找所有 NodeExecutor<?> Bean，如果只有一个，假设它是通用的或用于默认上下文。
     * 如果有多个，尝试找到那个在其依赖链中包含特定 NodeRegistry<C> 的实例。这比较复杂且脆弱。
     *
     * 最实用策略：要求用户为每个上下文类型 C 定义一个明确的 NodeExecutor<C> Bean，并能以某种方式识别它。
     * 例如，通过 Bean 名称约定，或者让 NodeExecutor 实现一个接口能返回它服务的 Context Type。
     *
     * 再次思考：StandardNodeExecutor<C> 构造时需要 NodeRegistry<C>。我们可以查找所有 NodeExecutor Bean，
     * 检查它们内部引用的 NodeRegistry 的 contextType 是否匹配。这需要访问 Bean 的内部状态，不太理想。
     *
     * 决定采用的策略：查找所有 NodeExecutor Bean。如果只有一个，假设它适用。如果有多个，抛出异常，
     * 要求用户明确指定或使用更高级的限定符。或者，查找与 NodeRegistry<C> Bean 在同一配置类中定义的 Executor Bean？
     *
     * 最终策略：查找所有 NodeExecutor Bean。遍历它们，尝试找到一个其构造函数参数或字段（通过反射，不推荐）
     * 包含我们需要的 NodeRegistry<C> 实例。
     *
     * 更简单的策略：查找所有 NodeExecutor Bean。如果只有一个，用它。如果有多个，抛异常，提示用户需要明确配置。
     * 或者，查找名为 "contextTypeSimpleName + NodeExecutor" 的 Bean？ (约定优于配置)
     *
     * 决定采用：查找所有 NodeExecutor Bean。过滤出类型为 NodeExecutor<C> 的 Bean。
     * 这需要 Spring 的泛型解析能力。`applicationContext.getBeanProvider(ResolvableType.forClassWithGenerics(NodeExecutor.class, contextType))`
     */
    @SuppressWarnings("unchecked")
    private <C> NodeExecutor<C> findExecutorForContext(Class<C> contextType) {
        // 尝试从缓存获取
        NodeExecutor<?> cachedExecutor = executorCache.get(contextType);
        if (cachedExecutor != null) {
            return (NodeExecutor<C>) cachedExecutor;
        }

        // 使用 BeanFactory/ApplicationContext 查找特定泛型类型的 Bean
        // 这依赖于 Spring 如何注册和解析泛型 Bean 定义
        // 通常用户需要这样定义 Bean: @Bean public NodeExecutor<MyContext> myContextExecutor(...)
        org.springframework.core.ResolvableType requiredType =
                org.springframework.core.ResolvableType.forClassWithGenerics(NodeExecutor.class, contextType);

        String[] beanNames = applicationContext.getBeanNamesForType(requiredType);

        if (beanNames.length == 0) {
            // 尝试查找通用的 NodeExecutor (没有泛型参数的)
            String[] genericBeanNames = applicationContext.getBeanNamesForType(NodeExecutor.class);
            if (genericBeanNames.length == 1) {
                log.warn("No specific NodeExecutor<{}> bean found. Falling back to the single generic NodeExecutor bean found: {}",
                        contextType.getSimpleName(), genericBeanNames[0]);
                NodeExecutor<?> genericExecutor = (NodeExecutor<?>) applicationContext.getBean(genericBeanNames[0]);
                // 缓存所有找到的 executor
                // 注意：这里无法确定通用 executor 的真实上下文类型，缓存可能不准确
                // executorCache.putIfAbsent(???, genericExecutor);
                return (NodeExecutor<C>) genericExecutor; // 强制转换，风险在于类型不匹配
            } else if (genericBeanNames.length > 1) {
                throw new IllegalStateException(
                        String.format("No specific NodeExecutor bean found for context type '%s', and multiple generic NodeExecutor beans exist: [%s]. " +
                                        "Please define a specific NodeExecutor<%s> bean or ensure only one generic NodeExecutor exists.",
                                contextType.getSimpleName(), String.join(", ", genericBeanNames), contextType.getSimpleName()));
            } else {
                // 没有任何 NodeExecutor Bean
                throw new NoSuchBeanDefinitionException(requiredType,
                        "No NodeExecutor bean found for context type '" + contextType.getSimpleName() +
                                "' and no generic NodeExecutor bean found either. Please define a NodeExecutor bean.");
            }
        } else if (beanNames.length > 1) {
            // 找到多个特定类型的 Executor，配置错误
            throw new IllegalStateException(
                    String.format("Multiple NodeExecutor beans found for context type '%s': [%s]. Please ensure only one bean per context type.",
                            contextType.getSimpleName(), String.join(", ", beanNames)));
        } else {
            // 找到了唯一匹配的特定类型 Executor
            NodeExecutor<C> foundExecutor = (NodeExecutor<C>) applicationContext.getBean(beanNames[0]);
            log.debug("Found specific NodeExecutor bean for context type '{}': {}", contextType.getSimpleName(), beanNames[0]);
            // 缓存起来
            executorCache.put(contextType, foundExecutor);
            return foundExecutor;
        }
    }

    /**
     * 清除内部缓存。主要用于测试或重新加载配置。
     */
    public void clearCache() {
        log.warn("Clearing DagEngineProvider cache.");
        engineCache.clear();
        registryCache.clear();
        executorCache.clear();
    }
}
