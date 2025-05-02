// [文件名称]: EdgeDefinition.java
package xyz.vvrf.reactor.dag.core;

import lombok.Getter;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;

/**
 * 代表 DAG 中的一条边（依赖关系），包含依赖源节点和执行条件。
 *
 * @param <C> 上下文类型
 */
public final class EdgeDefinition<C> {

    @Getter
    private final String dependencyNodeName; // 依赖的源节点名称
    @Getter
    private final BiPredicate<C, NodeResult<C, ?>> condition; // 边的执行条件
    private final boolean isDefaultCondition; // 标记是否使用了默认条件

    /**
     * 默认条件：当依赖节点成功执行时，条件满足。
     * 注意：类型为 Object 以便静态共享，使用时需要转换。
     */
    @SuppressWarnings("rawtypes") // 允许原始类型以便静态初始化
    private static final BiPredicate DEFAULT_CONDITION_RAW =
            (context, dependencyResult) -> dependencyResult != null && ((NodeResult)dependencyResult).isSuccess();


    /**
     * 创建一个具有默认条件的边定义。
     * 默认条件是依赖节点成功完成 (isSuccess)。
     *
     * @param dependencyNodeName 依赖的源节点名称
     */
    public EdgeDefinition(String dependencyNodeName) {
        this(dependencyNodeName, null); // 使用 null 标记来应用默认条件
    }

    /**
     * 创建一个具有自定义条件的边定义。
     *
     * @param dependencyNodeName 依赖的源节点名称
     * @param condition          边的执行条件 Predicate，接收 Context 和 依赖节点的 NodeResult。
     *                           如果为 null，将使用默认条件 (依赖节点成功)。
     */
    @SuppressWarnings("unchecked") // 类型转换是安全的
    public EdgeDefinition(String dependencyNodeName, BiPredicate<C, NodeResult<C, ?>> condition) {
        this.dependencyNodeName = Objects.requireNonNull(dependencyNodeName, "依赖节点名称不能为空");
        if (condition == null) {
            // 使用类型安全的转换来获取默认条件
            this.condition = (BiPredicate<C, NodeResult<C, ?>>) DEFAULT_CONDITION_RAW;
            this.isDefaultCondition = true; // 设置标志位
        } else {
            this.condition = condition;
            this.isDefaultCondition = false; // 清除标志位
        }
    }

    /**
     * 检查此边是否使用了默认条件。
     * @return 如果使用了默认条件则返回 true。
     */
    public boolean isDefaultCondition() {
        return isDefaultCondition;
    }

    /**
     * 评估此边的条件是否满足。
     *
     * @param context          当前上下文
     * @param dependencyResult 依赖节点的执行结果
     * @return 如果条件满足，则返回 true；否则返回 false。
     * @throws RuntimeException 如果条件 Predicate 执行时抛出异常。
     */
    public boolean evaluateCondition(C context, NodeResult<C, ?> dependencyResult) {
        try {
            if (dependencyResult == null) {
                System.err.printf("警告: 评估边条件时，依赖节点 '%s' 的结果为 null%n", dependencyNodeName);
                return false;
            }
            return condition.test(context, dependencyResult);
        } catch (Exception e) {
            throw new RuntimeException(String.format("评估节点 '%s' 的依赖条件时出错", dependencyNodeName), e);
        }
    }

    @Override
    public String toString() {
        // 使用 isDefaultCondition() 判断
        return "EdgeDefinition{" +
                "dependency='" + dependencyNodeName + '\'' +
                ", condition=" + (isDefaultCondition() ? "DEFAULT(onSuccess)" : "CUSTOM") +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EdgeDefinition<?> that = (EdgeDefinition<?>) o;
        // 比较依赖名称和 isDefault 标志，以及非默认时的 condition 对象
        return dependencyNodeName.equals(that.dependencyNodeName) &&
                isDefaultCondition == that.isDefaultCondition &&
                Objects.equals(condition, that.condition); // 对于非默认情况，比较 condition 实例
    }

    @Override
    public int hashCode() {
        // 包含 isDefault 标志
        return Objects.hash(dependencyNodeName, isDefaultCondition, condition);
    }
}
