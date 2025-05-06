package xyz.vvrf.reactor.dag.core;

import java.util.Set;

/**
 * 最复杂的条件类型，依赖于当前上下文、直接上游节点结果，以及显式声明依赖的其他节点结果。
 * 应谨慎使用，仅用于无法通过前两种条件类型表达的特殊场景。
 *
 * @param <C> 上下文类型
 * @author 重构者
 */
public interface DeclaredDependencyCondition<C> extends ConditionBase<C> {

    /**
     * 基于上下文和允许访问的节点结果评估条件。
     *
     * @param context 当前上下文对象
     * @param inputs  一个受限的访问器，提供对直接上游和声明依赖节点结果的访问能力
     * @return 如果条件满足，则返回 true；否则返回 false。
     */
    boolean evaluate(C context, ConditionInputAccessor<C> inputs);

    /**
     * 声明此条件除了直接上游节点外，还需要访问哪些其他节点实例的结果。
     * 引擎将使用此信息来提供一个受限制的 {@link ConditionInputAccessor}。
     * **实现此接口必须提供非空的依赖集合（如果确实需要额外依赖）。**
     *
     * @return 一个包含额外所需节点实例名称的不可变集合。不能为空。
     */
    Set<String> getRequiredNodeDependencies();
}
