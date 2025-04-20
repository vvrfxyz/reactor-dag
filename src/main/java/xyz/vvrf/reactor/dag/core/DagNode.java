package xyz.vvrf.reactor.dag.core;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * DAG节点接口 - 定义节点的基本能力
 *
 * @param <C> 上下文类型 (Context Type)
 * @param <P> 节点输出的 Payload 类型 (Payload Type)
 * @param <T> 节点产生的事件数据类型 (Event Data Type)
 * @author ruifeng.wen
 */
public interface DagNode<C, P, T> {

    /**
     * 获取节点名称，默认为类名
     * @return 节点名称
     */
    default String getName() {
        return getClass().getSimpleName();
    }

    /**
     * 获取节点依赖列表
     *
     * @return 依赖描述符列表
     */
    List<DependencyDescriptor> getDependencies();

    /**
     * 获取节点输出的 Payload 类型
     *
     * @return Payload 类型的Class对象
     */
     Class<P> getPayloadType();

    /**
     * 获取节点产生的事件数据类型 (新增)
     *
     * @return Event 类型的Class对象
     */
    Class<T> getEventType();

    /**
     * 执行节点逻辑
     *
     * @param context 上下文对象
     * @param dependencyResults 依赖节点的执行结果 (Key: 依赖节点名称, Value: 依赖节点的 NodeResult)
     *                          依赖节点的 Payload 和 Event 类型是未知的 (用 ? 表示)。
     * @return 包含执行结果 (Payload 和/或 Events) 的 Mono
     */
    Mono<NodeResult<C, P, T>> execute(C context, Map<String, NodeResult<C, ?, ?>> dependencyResults);

    /**
     * 获取节点执行超时，默认为null（使用系统默认值）
     *
     * @return 超时时间，如果不指定则返回null
     */
    default Duration getExecutionTimeout() {
        return null;
    }
}
