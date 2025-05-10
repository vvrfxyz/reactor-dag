package xyz.vvrf.reactor.dag.core;

import java.util.Objects;

/**
 * 最简单的条件类型，仅依赖于直接上游节点的结果和当前上下文。
 * 这是最推荐使用的条件类型。
 *
 * @param <C> 上下文类型
 * @param <U> 直接上游节点的 Payload 类型
 * @author ruifeng.wen
 */
@FunctionalInterface
public interface DirectUpstreamCondition<C, U> extends ConditionBase<C> {

    /**
     * 基于直接上游节点的结果和上下文评估条件。
     *
     * @param context        当前上下文对象
     * @param upstreamResult 直接上游节点的执行结果 (NodeResult)
     * @return 如果条件满足，则返回 true；否则返回 false。
     */
    boolean evaluate(C context, NodeResult<C, U> upstreamResult);

    /**
     * 一个始终为 true 的条件实例 (不关心上游类型)。
     * 表示边总是激活（除非上游失败）。
     * 这是边的默认条件。
     */
    @SuppressWarnings("unchecked")
    static <C> DirectUpstreamCondition<C, Object> alwaysTrue() {
        return AlwaysTrueCondition.INSTANCE;
    }

    class AlwaysTrueCondition<C, U> implements DirectUpstreamCondition<C, U> {
        @SuppressWarnings("rawtypes")
        private static final AlwaysTrueCondition INSTANCE = new AlwaysTrueCondition<>();

        private AlwaysTrueCondition() {}

        @Override
        public boolean evaluate(C context, NodeResult<C, U> upstreamResult) {
            // 仅当上游成功时，边才可能激活（引擎会处理上游失败/跳过的情况）
            // 这里简单返回 true，表示条件本身满足
            return true;
        }

        @Override
        public String toString() {
            return "Condition.alwaysTrue";
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof AlwaysTrueCondition;
        }

        @Override
        public int hashCode() {
            return AlwaysTrueCondition.class.hashCode();
        }
    }
}
