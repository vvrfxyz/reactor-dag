package xyz.vvrf.reactor.dag.core;

/**
 * 中等复杂度的条件类型，依赖于当前上下文和目标下游节点的所有直接上游节点的结果。
 * 适用于需要联合判断多个直接输入来源的场景。
 *
 * @param <C> 上下文类型
 * @author 重构者
 */
@FunctionalInterface
public interface LocalInputCondition<C> extends ConditionBase<C> {

    /**
     * 基于上下文和所有直接上游节点的结果评估条件。
     *
     * @param context     当前上下文对象
     * @param localInputs 一个访问器，提供对所有直接上游节点结果的访问能力
     * @return 如果条件满足，则返回 true；否则返回 false。
     */
    boolean evaluate(C context, LocalInputAccessor<C> localInputs);
}
