package xyz.vvrf.reactor.dag.core;

/**
 * reactor-dag
 *
 * @author ruifeng.wen
 * @date 5/2/25
 */

import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;
import java.time.Duration;

/**
 * 代表可复用的 DAG 节点核心业务逻辑。
 * 节点实例的具体名称、依赖、超时、重试等在 DAG 定义时配置。
 *
 * @param <C> 上下文类型 (Context Type)
 * @param <T> 节点产生的事件数据类型 (Event Data Type)
 */
public interface NodeLogic<C, T> {

    /**
     * 获取此逻辑实现的唯一标识符（可选，但推荐）。
     * 可用于日志、监控或查找。
     *
     * @return 逻辑标识符，例如类名或特定业务名称。
     */
    default String getLogicIdentifier() {
        return this.getClass().getSimpleName();
    }

    /**
     * 获取节点产生的事件数据类型。
     *
     * @return Event 类型的 Class 对象。
     */
    Class<T> getEventType();

    /**
     * 执行节点的核心逻辑。
     * 实现者应该：
     * 1. 从 context 读取所需信息。
     * 2. 使用 dependencyAccessor 检查依赖状态（如果需要）。
     * 3. 执行业务操作，可能会修改 context。
     * 4. 生成事件流 Flux<Event<T>>。
     * 5. 如果需要在事件流处理完成后执行额外操作（如聚合、保存），请编排该逻辑。
     * 6. 创建 NodeResult<C, T>，包含事件流。
     * 7. 返回 Mono<NodeResult<C, T>>，该 Mono 的完成信号表示此逻辑单元的工作（包括可能的后处理）已结束。
     *
     * @param context      共享的上下文对象，用于读写数据。注意并发安全。
     * @param dependencies 依赖节点的执行结果访问器。
     * @return 包含执行结果 (主要是事件流) 的 Mono。Mono 的完成表示节点逻辑单元结束。
     */
    Mono<NodeResult<C, T>> execute(C context, DependencyAccessor<C> dependencies);

    /**
     * 定义此逻辑的默认重试策略。
     * 可以在 DagNodeDefinition 中被覆盖。
     *
     * @return Reactor 的 Retry 规范，或 null/Retry.max(0) 表示不重试。
     */
    default Retry getDefaultRetrySpec() {
        return null;
    }

    /**
     * 定义此逻辑的默认执行超时。
     * 可以在 DagNodeDefinition 中被覆盖。
     *
     * @return 默认超时时间，null 表示使用引擎的默认值。
     */
    default Duration getDefaultExecutionTimeout() {
        return null;
    }

    /**
     * 判断此逻辑单元是否应该基于当前上下文和依赖状态执行。
     * 可以在 DagNodeDefinition 中被覆盖或组合。
     *
     * @param context      当前上下文对象。
     * @param dependencies 依赖节点的执行结果访问器。
     * @return 如果逻辑应该执行，则返回 true；否则返回 false。
     */
    default boolean shouldExecute(C context, DependencyAccessor<C> dependencies) {
        return true;
    }
}
