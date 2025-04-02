package xyz.vvrf.reactor.dag.core;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * DAG节点接口 - 定义节点的基本能力
 *
 * @param <C> 上下文类型
 * @param <T> 节点输出的负载类型
 * @author ruifeng.wen
 */
public interface DagNode<C, T> {

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
     * 获取节点输出的负载类型
     *
     * @return 负载类型的Class对象
     */
    Class<T> getPayloadType();

    /**
     * 执行节点逻辑
     *
     * @param context 上下文对象
     * @param dependencyResults 依赖节点的执行结果
     * @return 包含执行结果的Mono
     */
    Mono<NodeResult<C, T>> execute(C context, Map<String, NodeResult<C, ?>> dependencyResults);

    /**
     * 获取节点执行超时，默认为null（使用系统默认值）
     *
     * @return 超时时间，如果不指定则返回null
     */
    default Duration getExecutionTimeout() {
        return null;
    }
}