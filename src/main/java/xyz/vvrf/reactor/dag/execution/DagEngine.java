// file: execution/DagEngine.java
package xyz.vvrf.reactor.dag.execution;

import reactor.core.publisher.Flux;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.Event;

import java.util.UUID; // 引入 UUID

/**
 * DAG 执行引擎接口。
 *
 * @param <C> 上下文类型
 * @author Refactored
 */
public interface DagEngine<C> {

    /**
     * 执行指定的 DAG 定义。
     *
     * @param initialContext 初始上下文对象
     * @param dagDefinition  要执行的 DAG 定义
     * @param requestId      可选的请求 ID，用于日志和监控。如果为 null 或空，将自动生成。
     * @return 合并所有成功节点事件流的 Flux<Event<?>>。如果 DAG 执行失败（取决于策略），Flux 可能会以错误终止。
     */
    Flux<Event<?>> execute(C initialContext, DagDefinition<C> dagDefinition, String requestId);

    /**
     * 执行指定的 DAG 定义，自动生成请求 ID。
     *
     * @param initialContext 初始上下文对象
     * @param dagDefinition  要执行的 DAG 定义
     * @return 合并所有成功节点事件流的 Flux<Event<?>>。
     */
    default Flux<Event<?>> execute(C initialContext, DagDefinition<C> dagDefinition) {
        // 生成更可读的默认 ID
        return execute(initialContext, dagDefinition, "dag-req-" + UUID.randomUUID().toString().substring(0, 8));
    }
}
