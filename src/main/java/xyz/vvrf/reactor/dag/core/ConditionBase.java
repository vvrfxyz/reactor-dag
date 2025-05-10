package xyz.vvrf.reactor.dag.core;

/**
 * 所有条件接口的基础标记接口。
 * 用于在 EdgeDefinition 中统一存储不同类型的条件。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen
 */
public interface ConditionBase<C> {
    /**
     * 获取条件的易读类型名称，主要用于日志和调试。
     *
     * @return 条件的显示名称字符串。
     */
    String getConditionTypeDisplayName();
}
