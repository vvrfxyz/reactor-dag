// 文件名: annotation/DagNodeType.java (新文件)
package xyz.vvrf.reactor.dag.annotation;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * 标记一个类为可被发现的 DAG 节点实现。
 * 使用此注解的类如果其 {@link #contextType()} 与注册表的上下文匹配，
 * 将会被 {@link xyz.vvrf.reactor.dag.registry.SpringScanningNodeRegistry} 自动注册。
 * <p>
 * 包含 {@link Component} 以便 Spring 在组件扫描期间自动检测这些类。
 *
 * @author Refactored
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
@Component // 使其成为 Spring 组件以便扫描
public @interface DagNodeType {

    /**
     * 节点类型 ID，同时也是 {@link #id()} 和 {@link Component#value()} 的别名。
     *
     * @return 节点类型 ID。
     */
    @AliasFor("id") // 明确 value 是 id 的别名
            String value() default "";

    /**
     * 节点类型 ID，同时也是 {@link #value()} 的别名。
     *
     * @return 节点类型 ID。
     */
    @AliasFor("value") // 明确 id 是 value 的别名
            String id() default "";

    /**
     * 此节点实现所操作的特定上下文类。
     *
     * @return 上下文类。
     */
    Class<?> contextType();

    /**
     * 此节点的 Spring bean 定义的作用域。
     *
     * @return bean 作用域。
     */
    String scope() default BeanDefinition.SCOPE_SINGLETON;

}