package xyz.vvrf.reactor.dag.core;

/**
 * 定义边的激活条件。
 *
 * @param <C> 上下文类型
 * @author Refactored
 */
@FunctionalInterface
public interface Condition<C> {

    /**
     * 评估条件是否满足。
     *
     * @param context 当前上下文对象
     * @param inputs  一个特殊的 InputAccessor，提供对 *所有* 已完成（成功、失败或跳过）的上游节点结果的访问能力。
     *                实现者应谨慎使用，避免引入不必要的复杂依赖。通常只关心直接上游或少数几个关键节点的状态/结果。
     * @return 如果条件满足，则返回 true，边被视为激活；否则返回 false。
     */
    boolean evaluate(C context, ConditionInputAccessor<C> inputs);

    /**
     * 一个始终为 true 的条件，表示边总是激活（除非上游失败）。
     */
    static <C> Condition<C> alwaysTrue() {
        return (ctx, inputs) -> true;
    }
}