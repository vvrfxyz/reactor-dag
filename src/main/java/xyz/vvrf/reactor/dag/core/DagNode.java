// file: core/DagNode.java
package xyz.vvrf.reactor.dag.core;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

/**
 * DAG 逻辑节点接口 - 定义节点的基本可复用逻辑单元。
 * 节点实例的名称和依赖连接在构建图时定义。
 * 节点声明其输入需求。
 *
 * @param <C> 上下文类型 (Context Type)
 * @param <P> 节点输出的 Payload 类型 (Payload Type)
 * @author ruifeng.wen (Refactored)
 */
public interface DagNode<C, P> {

    /**
     * 获取此节点逻辑的输入需求。
     * Key: 输入槽的逻辑名称 (例如 "userData", "orderInfo")
     * Value: 该输入槽期望接收的 Payload 类型
     *
     * @return 输入需求的 Map，如果没有输入则为空 Map。
     */
    default Map<String, Class<?>> getInputRequirements() {
        return Collections.emptyMap();
    }

    /**
     * 获取此节点的重试策略。
     * 返回 null 或 Retry.max(0) 表示不重试。
     *
     * @return Reactor 的 Retry 规范，或 null/Retry.max(0)
     */
    default Retry getRetrySpec() {
        return null;
    }

    /**
     * 判断此节点实例是否应该基于其输入数据来执行。
     * 如果返回 false，节点执行将被跳过。
     *
     * @param context 当前上下文对象
     * @param inputs  输入数据的访问器。
     * @return 如果节点应该执行，则返回 true；否则返回 false。
     */
    default boolean shouldExecute(C context, InputAccessor<C> inputs) {
        return true;
    }

    /**
     * 获取节点输出的 Payload 类型
     *
     * @return Payload 类型的Class对象
     */
    Class<P> getPayloadType();

    /**
     * 获取节点执行超时，默认为null（使用系统默认值）
     *
     * @return 超时时间，如果不指定则返回null
     */
    default Duration getExecutionTimeout() {
        return null;
    }

    /**
     * 执行节点逻辑。
     *
     * @param context 上下文对象
     * @param inputs  输入数据的访问器，用于安全地获取上游节点的产物。
     * @return 包含执行结果 (Payload 和/或 Events) 的 Mono
     */
    Mono<NodeResult<C, P>> execute(C context, InputAccessor<C> inputs);
}
