// 文件名: registry/SpringScanningNodeRegistry.java (新文件)
package xyz.vvrf.reactor.dag.registry;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.lang.NonNull;
import xyz.vvrf.reactor.dag.annotation.DagNodeType;
import xyz.vvrf.reactor.dag.core.DagNode;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * 一个 {@link NodeRegistry} 实现，它会自动发现并注册
 * 使用 {@link DagNodeType} 注解的 Spring Bean。
 *
 * 它在初始化后扫描 ApplicationContext，并注册那些
 * {@link DagNodeType#contextType()} 与本注册表创建时指定的上下文类型匹配的节点。
 *
 * @param <C> 本注册表管理的上下文类型。
 * @author Refactored
 */
@Slf4j
public class SpringScanningNodeRegistry<C> implements NodeRegistry<C>, ApplicationContextAware, InitializingBean {

    private final Class<C> contextType;
    private ApplicationContext applicationContext;
    // 内部使用 SimpleNodeRegistry 来存储注册信息和元数据
    private final SimpleNodeRegistry<C> delegateRegistry;

    /**
     * 创建一个新的 SpringScanningNodeRegistry。
     * @param contextType 本注册表负责的特定上下文类 (例如 OrderContext.class)。不能为空。
     */
    public SpringScanningNodeRegistry(Class<C> contextType) {
        this.contextType = Objects.requireNonNull(contextType, "上下文类型不能为空");
        this.delegateRegistry = new SimpleNodeRegistry<>(contextType); // 初始化内部委托
        log.info("为上下文类型 '{}' 创建了 SpringScanningNodeRegistry", contextType.getSimpleName());
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (applicationContext == null) {
            throw new BeanCreationException("在 SpringScanningNodeRegistry (上下文: " + contextType.getSimpleName() + ") 中 ApplicationContext 未设置");
        }
        log.info("开始为上下文 '{}' 扫描 @DagNodeType Bean...", contextType.getSimpleName());
        scanAndRegisterNodes();
    }

    private void scanAndRegisterNodes() {
        // 查找所有带有 @DagNodeType 注解的 Bean
        Map<String, Object> beansWithAnnotation = applicationContext.getBeansWithAnnotation(DagNodeType.class);
        int registeredCount = 0;

        for (Map.Entry<String, Object> entry : beansWithAnnotation.entrySet()) {
            String beanName = entry.getKey();
            Object beanInstance = entry.getValue();
            DagNodeType annotation = applicationContext.findAnnotationOnBean(beanName, DagNodeType.class);

            if (annotation == null) {
                log.warn("在 Bean '{}' 上找不到 @DagNodeType 注解，尽管 getBeansWithAnnotation 返回了它。", beanName);
                continue;
            }

            // 检查上下文类型是否与本注册表匹配
            if (annotation.contextType() == this.contextType) {
                String nodeTypeId = determineNodeTypeId(annotation, beanName);
                String scope = annotation.scope();

                if (!(beanInstance instanceof DagNode)) {
                    log.error("Bean '{}' 使用了 @DagNodeType (上下文: '{}') 注解，但未实现 DagNode 接口。跳过注册。",
                            beanName, contextType.getSimpleName());
                    continue;
                }

                // 注意：由于类型擦除，我们无法在运行时轻松完美检查 DagNode<C, P> 的 C。
                // 我们主要依赖 annotation.contextType() 检查。

                try {
                    if (BeanDefinition.SCOPE_PROTOTYPE.equals(scope)) {
                        // 注册一个从上下文中获取原型 Bean 的工厂
                        log.debug("注册原型节点: ID='{}', 上下文='{}', Bean名='{}'", nodeTypeId, contextType.getSimpleName(), beanName);
                        Supplier<DagNode<C, ?>> factory = () -> {
                            // 每次都从上下文中获取新实例
                            Object prototypeBean = applicationContext.getBean(beanName);
                            if (!(prototypeBean instanceof DagNode)) {
                                throw new IllegalStateException("原型 Bean " + beanName + " 不再是 DagNode 实例。");
                            }
                            @SuppressWarnings("unchecked") // 类型转换是安全的
                            DagNode<C, ?> dagNodeInstance = (DagNode<C, ?>) prototypeBean;
                            return dagNodeInstance;
                        };
                        delegateRegistry.register(nodeTypeId, factory);
                    } else { // 默认为单例
                        log.debug("注册单例节点: ID='{}', 上下文='{}', Bean名='{}'", nodeTypeId, contextType.getSimpleName(), beanName);
                        @SuppressWarnings("unchecked") // 类型转换是安全的
                        DagNode<C, ?> singletonNode = (DagNode<C, ?>) beanInstance;
                        delegateRegistry.register(nodeTypeId, singletonNode);
                    }
                    registeredCount++;
                } catch (IllegalArgumentException e) {
                    // 记录注册错误（例如重复ID），但继续扫描
                    log.error("注册节点 Bean '{}' (ID: '{}', 上下文: '{}') 失败: {}",
                            beanName, nodeTypeId, contextType.getSimpleName(), e.getMessage());
                } catch (Exception e) {
                    log.error("注册节点 Bean '{}' (ID: '{}', 上下文: '{}') 时发生意外错误",
                            beanName, nodeTypeId, contextType.getSimpleName(), e);
                }
            }
        }
        log.info("上下文 '{}' 的扫描完成。共注册了 {} 个节点。", contextType.getSimpleName(), registeredCount);
    }

    private String determineNodeTypeId(DagNodeType annotation, String beanName) {
        String id = annotation.id();
        if (id.isEmpty()) {
            id = annotation.value();
        }
        if (id.isEmpty()) {
            // 如果注解中未提供 ID，则回退使用 Bean 名称
            log.warn("在 Bean '{}' 的 @DagNodeType 注解中未提供 'id' 或 'value'。将使用 Bean 名称作为节点类型 ID。", beanName);
            return beanName;
        }
        return id;
    }

    @Override
    public Class<C> getContextType() {
        return delegateRegistry.getContextType();
    }

    @Override
    public void register(String nodeTypeId, Supplier<? extends DagNode<C, ?>> factory) {
        // 仍然允许手动注册，但不推荐与扫描混合使用
        log.warn("尝试在 SpringScanningNodeRegistry 上手动注册 ID '{}'。推荐使用自动扫描。", nodeTypeId);
        delegateRegistry.register(nodeTypeId, factory);
    }

    @Override
    public void register(String nodeTypeId, DagNode<C, ?> prototype) {
        log.warn("尝试在 SpringScanningNodeRegistry 上手动注册 ID '{}'。推荐使用自动扫描。", nodeTypeId);
        delegateRegistry.register(nodeTypeId, prototype);
    }

    @Override
    public Optional<DagNode<C, ?>> getNodeInstance(String nodeTypeId) {
        return delegateRegistry.getNodeInstance(nodeTypeId);
    }

    @Override
    public Optional<NodeMetadata> getNodeMetadata(String nodeTypeId) {
        return delegateRegistry.getNodeMetadata(nodeTypeId);
    }
}
