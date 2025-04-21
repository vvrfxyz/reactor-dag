// [file name]: TestDagDefinition.java
package xyz.vvrf.reactor.dag.test.util;

import xyz.vvrf.reactor.dag.builder.ChainBuilder;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 用于测试目的的简单 DagDefinition 实现。
 * 继承自 AbstractDagDefinition 以利用其验证和排序逻辑。
 *
 * @param <C> 上下文类型
 */
public class TestDagDefinition<C> extends AbstractDagDefinition<C> {

    private final String dagName;

    /**
     * 创建 TestDagDefinition 实例。
     *
     * @param contextType 上下文类型
     * @param nodes       包含的节点列表
     * @param dagName     DAG 的名称 (可选, 默认为类名)
     */
    public TestDagDefinition(Class<C> contextType, List<DagNode<C, ?, ?>> nodes, String dagName) {
        super(contextType, nodes); // 调用父类构造函数注册节点
        this.dagName = (dagName != null && !dagName.trim().isEmpty()) ? dagName : getClass().getSimpleName();
        // 注意：初始化 (initialize()) 需要在显式依赖设置后手动调用，或者通过 initializeIfNeeded()
    }

    public TestDagDefinition(Class<C> contextType, List<DagNode<C, ?, ?>> nodes) {
        this(contextType, nodes, null);
    }

    @Override
    public String getDagName() {
        return this.dagName;
    }

    /**
     * 获取一个 ChainBuilder 来为此定义配置显式依赖。
     * 配置完成后，需要调用 {@link #applyExplicitDependencies(Map)} 和 {@link #initializeIfNeeded()}。
     *
     * @return ChainBuilder 实例
     */
    public ChainBuilder<C> chain() {
        return new ChainBuilder<>(this);
    }

    /**
     * 应用通过 ChainBuilder 构建的显式依赖。
     * 应在调用 initialize() 之前调用。
     *
     * @param explicitDependenciesMap 由 ChainBuilder.build() 生成的映射
     */
    public void applyExplicitDependencies(Map<String, List<xyz.vvrf.reactor.dag.core.DependencyDescriptor>> explicitDependenciesMap) {
        Objects.requireNonNull(explicitDependenciesMap, "显式依赖映射不能为空");
        explicitDependenciesMap.forEach(this::addExplicitDependencies); // 调用父类方法添加
    }

    /**
     * 方便的方法，用于构建链式依赖并立即应用和初始化。
     *
     * @param builderConfigurer 配置 ChainBuilder 的函数
     * @return this 实例 (已初始化)
     * @throws IllegalStateException 如果初始化失败
     */
    public TestDagDefinition<C> buildAndInitialize(java.util.function.Consumer<ChainBuilder<C>> builderConfigurer) {
        ChainBuilder<C> builder = chain();
        builderConfigurer.accept(builder);
        applyExplicitDependencies(builder.build());
        initializeIfNeeded(); // 确保初始化
        return this;
    }

    /**
     * 如果没有显式依赖，直接初始化。
     * @return this 实例 (已初始化)
     * @throws IllegalStateException 如果初始化失败
     */
    public TestDagDefinition<C> initializeNow() {
        initializeIfNeeded();
        return this;
    }
}
