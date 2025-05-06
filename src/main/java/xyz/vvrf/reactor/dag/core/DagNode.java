package xyz.vvrf.reactor.dag.core;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;

/**
 * DAG 逻辑节点接口 - 定义节点的基本可复用逻辑单元。
 * 节点声明其类型化的输入和输出槽。
 * 实例配置和连接在 DagDefinition 中定义。
 * 节点实现应该是线程安全的，特别是当它们作为原型（单例）注册时。
 *
 * @param <C> 上下文类型 (Context Type)
 * @param <P> 节点主输出的 Payload 类型 (Payload Type) - 对应默认 OutputSlot
 * @author Refactored (注释更新)
 */
public interface DagNode<C, P> {

    /**
     * 获取此节点逻辑声明的所有输入槽。
     *
     * @return 输入槽的不可变集合，如果没有输入则为空集。
     */
    Set<InputSlot<?>> getInputSlots();

    /**
     * 获取此节点逻辑声明的主输出槽 (默认输出)。
     *
     * @return 主输出槽。
     */
    OutputSlot<P> getOutputSlot();

    /**
     * (可选) 获取此节点逻辑声明的其他命名输出槽。
     *
     * @return 其他输出槽的不可变集合，如果没有则为空集。
     */
    default Set<OutputSlot<?>> getAdditionalOutputSlots() {
        return Collections.emptySet();
    }

    /**
     * 获取此节点的重试策略。
     * 返回 null 或 Retry.max(0) 表示不重试。
     *
     * @return Reactor 的 Retry 规范，或 null/Retry.max(0)
     */
    default Retry getRetrySpec() {
        return null; // 默认不重试
    }

    /**
     * 获取节点执行超时。
     * 如果返回 null 或非正数 Duration，将使用引擎的默认超时时间。
     *
     * @return 超时时间，如果不指定则返回 null
     */
    default Duration getExecutionTimeout() {
        return null;
    }

    /**
     * 执行节点逻辑。
     *
     * @param context 上下文对象
     * @param inputs  输入数据的访问器，用于安全地获取激活的入边提供的数据。
     * @return 包含执行结果 (Payload 和/或 Events) 的 Mono。Payload 对应主输出槽。
     *         如果执行过程中发生无法恢复的错误，应返回 Mono.error(exception)。
     *         如果节点希望表明逻辑失败（例如业务规则不满足），应返回 NodeResult.failure(error)。
     */
    Mono<NodeResult<C, P>> execute(C context, InputAccessor<C> inputs);

}
