package xyz.vvrf.reactor.dag.execution;

import reactor.core.publisher.Flux;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.Event;

import java.util.UUID;

/**
 * DAG 执行引擎接口。
 * 负责接收 DAG 定义和初始上下文，并执行 DAG。
 *
 * @param <C> 上下文类型
 * @author Refactored (注释更新)
 */
public interface DagEngine<C> {

    /**
     * 执行指定的 DAG 定义。
     *
     * @param initialContext 初始上下文对象 (不能为空)
     * @param dagDefinition  要执行的 DAG 定义 (不能为空)
     * @param requestId      可选的请求 ID，用于日志和监控。如果为 null 或空，将自动生成。
     * @return 一个 Flux<Event<?>>，它会发出 DAG 中所有成功执行的节点产生的事件。
     *         如果 DAG 执行失败（取决于错误处理策略），Flux 可能会以错误终止 (onError)。
     *         如果 DAG 成功完成（即使某些节点被跳过或在 CONTINUE_ON_FAILURE 策略下失败），Flux 会正常完成 (onComplete)。
     */
    Flux<Event<?>> execute(C initialContext, DagDefinition<C> dagDefinition, String requestId);

    /**
     * 执行指定的 DAG 定义，自动生成请求 ID。
     *
     * @param initialContext 初始上下文对象 (不能为空)
     * @param dagDefinition  要执行的 DAG 定义 (不能为空)
     * @return 合并所有成功节点事件流的 Flux<Event<?>>。
     */
    default Flux<Event<?>> execute(C initialContext, DagDefinition<C> dagDefinition) {
        // 生成一个易于识别的默认请求 ID
        String defaultRequestId = "dag-req-" + UUID.randomUUID().toString().substring(0, 8);
        return execute(initialContext, dagDefinition, defaultRequestId);
    }
}
