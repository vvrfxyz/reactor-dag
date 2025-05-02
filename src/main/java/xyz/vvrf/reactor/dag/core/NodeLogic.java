// [文件名称]: NodeLogic.java
package xyz.vvrf.reactor.dag.core;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import java.time.Duration;
// 移除 Map 和 shouldExecute 相关导入

/**
 * 代表可复用的 DAG 节点核心业务逻辑。
 * 节点实例的具体名称、依赖（通过边定义）、超时、重试等在 DAG 定义时配置。
 * 节点执行时，应从共享的 Context 读取输入数据，并将需要给下游节点使用的数据写回 Context。
 *
 * @param <C> 上下文类型 (Context Type)，必须设计为线程安全或有效不可变。
 * @param <T> 节点产生的事件数据类型 (Event Data Type)，用于最终的 DAG 输出流。
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
     * 这个类型主要关联到最终输出的 Event 流。
     *
     * @return Event 数据类型的 Class 对象。
     */
    Class<T> getEventType();

    /**
     * 执行节点的核心逻辑。
     * 实现者应该：
     * 1. 从 context 读取所需信息（包括上游节点写入的数据）。
     * 2. 执行业务操作。
     * 3. 将产生的、需要被下游节点使用的数据写回 context (注意并发安全)。
     * 4. 生成用于最终输出的事件流 Flux<Event<T>>。
     * 5. 创建 NodeResult<C, T>，包含状态和事件流。
     * 6. 返回 Mono<NodeResult<C, T>>，该 Mono 的完成信号表示此逻辑单元的工作已结束。
     *
     * @param context 共享的上下文对象，用于读写数据。实现者必须保证对其访问的线程安全。
     * @return 包含执行结果 (状态和最终输出事件流) 的 Mono。Mono 的完成表示节点逻辑单元结束。
     */
    Mono<NodeResult<C, T>> execute(C context);

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

    // 移除 shouldExecute 方法
    // default boolean shouldExecute(C context, Map<String, NodeResult<C, ?>> dependencyResults) { ... }
}
