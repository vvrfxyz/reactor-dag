package xyz.vvrf.reactor.dag.core;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * DAG节点接口 - 定义节点的基本能力。
 * 节点通过 getInputRequirements() 声明其输入数据需求 (类型和可选限定符)。
 * 节点的执行顺序依赖关系通过外部配置（如 ChainBuilder）显式定义。
 *
 * @param <C> 上下文类型 (Context Type)
 * @param <P> 节点输出的 Payload 类型 (Payload Type)
 * @param <T> 节点产生的事件数据类型 (Event Data Type)
 * @author ruifeng.wen
 */
public interface DagNode<C, P, T> {

    /**
     * 获取节点的唯一名称。
     * 此名称在 DAG 定义中必须是唯一的。
     * 默认实现返回类的简单名称，但强烈建议实现类覆盖此方法以提供稳定且有意义的名称。
     *
     * @return 节点的唯一名称
     */
    default String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * 获取此节点的重试策略。
     * 返回 null 或 Retry.max(0) 表示不重试。
     *
     * @return Reactor 的 Retry 规范，或 null/Retry.max(0)
     * @see reactor.util.retry.Retry
     * @see reactor.util.retry.RetryBackoffSpec
     */
    default Retry getRetrySpec() {
        return null;
    }

    /**
     * 判断此节点是否应该基于其依赖项的结果来执行。
     * 如果返回 false，节点执行将被跳过，并可能产生一个表示“跳过”的 NodeResult。
     *
     * @param context      当前上下文对象
     * @param dependencies 依赖节点的执行结果访问器 (基于类型/限定符)。
     *                     此访问器现在可以访问所有已完成的上游节点结果，不仅仅是直接前驱。
     * @return 如果节点应该执行，则返回 true；否则返回 false。
     */
    default boolean shouldExecute(C context, InputDependencyAccessor<C> dependencies) {
        return true;
    }

    /**
     * 获取节点输出的 Payload 类型
     *
     * @return Payload 类型的Class对象
     */
    Class<P> getPayloadType();

    /**
     * 获取节点产生的事件数据类型
     *
     * @return Event 类型的Class对象
     */
    Class<T> getEventType();

    /**
     * 获取节点执行超时，默认为null（使用系统默认值）
     *
     * @return 超时时间，如果不指定则返回null
     */
    default Duration getExecutionTimeout() {
        return null;
    }

    /**
     * 声明此节点执行所需的输入数据。
     * 框架将根据 DAG 配置和已完成节点的结果来满足这些需求。
     *
     * @return 输入需求列表 (默认为空)
     */
    default List<InputRequirement<?>> getInputRequirements() {
        return Collections.emptyList();
    }

    /**
     * 执行节点逻辑。
     *
     * @param context      上下文对象
     * @param dependencies 依赖节点的执行结果访问器，用于安全地获取上游节点的产物 (基于类型/限定符)。
     *                     此访问器现在可以访问所有已完成的上游节点结果，不仅仅是直接前驱。
     * @return 包含执行结果 (Payload 和/或 Events) 的 Mono
     */
    Mono<NodeResult<C, P, T>> execute(C context, InputDependencyAccessor<C> dependencies);
}
