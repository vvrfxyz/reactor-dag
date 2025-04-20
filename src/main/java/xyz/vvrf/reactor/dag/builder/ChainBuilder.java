package xyz.vvrf.reactor.dag.builder; // 可以放在一个新的包里

import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.DagDefinition; // 引入接口
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.DependencyDescriptor;
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition; // 引入改造后的类

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DAG 链式构建器。
 * 用于以编程方式定义节点间的依赖关系，覆盖节点自身的 getDependencies()。
 * 需要配合改造后的 AbstractDagDefinition 使用，该 Definition 包含 addExplicitDependencies 方法。
 *
 * @author ruifeng.wen
 * @param <C> 上下文类型
 */
@Slf4j
public class ChainBuilder<C> {

    // 注意：这里需要引用 DagDefinition 接口，而不是具体的 AbstractDagDefinition，
    // 因为查找节点信息是接口的能力。但 addExplicitDependencies 是 AbstractDagDefinition 的方法。
    // 为了调用 addExplicitDependencies，构造函数还是需要 AbstractDagDefinition。
    // 或者，将 addExplicitDependencies 提升到接口（如果所有实现都需要支持的话）。
    // 目前维持原样，构造函数传入 AbstractDagDefinition。
    private final AbstractDagDefinition<C> dagDefinition; // 需要引用来查找节点信息和添加显式依赖
    private final Map<Class<? extends DagNode<C, ?, ?>>, List<DependencyDescriptor>> explicitDependenciesMap;

    /**
     * 创建 ChainBuilder 实例。
     *
     * @param dagDefinition 正在配置的 DagDefinition 实例。
     *                      必须是 AbstractDagDefinition 的子类，且已完成节点注册。
     */
    public ChainBuilder(AbstractDagDefinition<C> dagDefinition) {
        this.dagDefinition = Objects.requireNonNull(dagDefinition, "DagDefinition 不能为空");
        this.explicitDependenciesMap = new ConcurrentHashMap<>();
        log.info("[{}] DAG '{}': ChainBuilder 已创建。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());
    }

    /**
     * 添加一个节点到构建器，并定义它的显式依赖。
     * 如果一个节点被多次调用此方法，后续调用会覆盖之前的依赖设置。
     *
     * @param nodeClass          要添加或配置依赖的目标节点类。
     * @param dependencyNodeClasses 它所依赖的节点类数组。如果为空或不传，表示此节点在此链中无显式依赖（可能是起点）。
     * @return 当前 ChainBuilder 实例，支持链式调用。
     * @throws IllegalArgumentException 如果目标节点类或依赖的节点类未在 DagDefinition 中注册，
     *                                或无法获取其 PayloadType。
     * @throws NullPointerException 如果节点类为 null。
     */
    @SafeVarargs
    public final ChainBuilder<C> node(Class<? extends DagNode<C, ?, ?>> nodeClass, Class<? extends DagNode<C, ?, ?>>... dependencyNodeClasses) {
        Objects.requireNonNull(nodeClass, "目标节点类不能为空");
        String nodeName = getNodeNameFromClass(nodeClass); // 使用辅助方法获取名称

        // 确认目标节点已注册
        if (!dagDefinition.getNodeAnyType(nodeName).isPresent()) {
            String errorMsg = String.format("目标节点 '%s' (Class: %s) 未在 DagDefinition '%s' 中注册。",
                    nodeName, nodeClass.getName(), dagDefinition.getDagName());
            log.error("[{}] {}", dagDefinition.getContextType().getSimpleName(), errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        List<DependencyDescriptor> dependencies = new ArrayList<>();
        if (dependencyNodeClasses != null) {
            for (Class<? extends DagNode<C, ?, ?>> depClass : dependencyNodeClasses) {
                Objects.requireNonNull(depClass, "依赖节点类不能为空");
                String depNodeName = getNodeNameFromClass(depClass); // 使用辅助方法获取名称

                // 从 DagDefinition 中查找依赖节点实例以获取其 PayloadType
                // 使用 getNodeAnyType，因为我们只关心它的 PayloadType
                Optional<DagNode<C, ?, ?>> depNodeOpt = dagDefinition.getNodeAnyType(depNodeName);
                if (!depNodeOpt.isPresent()) {
                    String errorMsg = String.format("节点 '%s' 的依赖节点 '%s' (Class: %s) 未在 DagDefinition '%s' 中注册。",
                            nodeName, depNodeName, depClass.getName(), dagDefinition.getDagName());
                    log.error("[{}] {}", dagDefinition.getContextType().getSimpleName(), errorMsg);
                    throw new IllegalArgumentException(errorMsg);
                }

                DagNode<C, ?, ?> depNode = depNodeOpt.get();
                Class<?> requiredType = depNode.getPayloadType();
                if (requiredType == null) {
                    // 理论上 registerNodes 时已检查，但再次确认
                    String errorMsg = String.format("节点 '%s' 的依赖节点 '%s' (Class: %s) 未提供有效的 PayloadType。",
                            nodeName, depNodeName, depClass.getName());
                    log.error("[{}] {}", dagDefinition.getContextType().getSimpleName(), errorMsg);
                    throw new IllegalArgumentException(errorMsg);
                }

                dependencies.add(new DependencyDescriptor(depNodeName, requiredType));
            }
        }

        log.debug("[{}] DAG '{}': Builder 定义节点 '{}' (Class: {}) 依赖于: {}",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                nodeName, nodeClass.getSimpleName(), dependencies);
        // 使用 Class 作为 Key 存储临时的依赖关系
        explicitDependenciesMap.put(nodeClass, dependencies);
        return this;
    }

    /**
     * 构建最终的显式依赖映射。
     * 将内部以 Class 为键的映射转换为以节点名称字符串为键的映射。
     *
     * @return Map<String, List<DependencyDescriptor>>，可用于调用 DagDefinition 的 addExplicitDependencies。
     */
    public Map<String, List<DependencyDescriptor>> build() {
        Map<String, List<DependencyDescriptor>> finalDependencies = new HashMap<>();
        for (Map.Entry<Class<? extends DagNode<C, ?, ?>>, List<DependencyDescriptor>> entry : explicitDependenciesMap.entrySet()) {
            String nodeName = getNodeNameFromClass(entry.getKey()); // 使用辅助方法获取名称
            // 再次确认节点存在，以防万一
            if (dagDefinition.getNodeAnyType(nodeName).isPresent()) {
                finalDependencies.put(nodeName, entry.getValue());
            } else {
                log.warn("[{}] DAG '{}': 在构建最终依赖映射时，节点 '{}' (Class: {}) 已不再注册，其显式依赖将被忽略。",
                        dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                        nodeName, entry.getKey().getName());
            }
        }
        log.info("[{}] DAG '{}': ChainBuilder 构建完成，生成了 {} 个节点的显式依赖配置。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                finalDependencies.size());
        return finalDependencies;
    }

    // --- 可选的便利方法 ---

    /**
     * 添加一个没有显式依赖的节点（例如，图的起点）。
     * 等同于调用 node(nodeClass)。
     *
     * @param nodeClass 起点节点类。
     * @return 当前 ChainBuilder 实例。
     */
    public ChainBuilder<C> startWith(Class<? extends DagNode<C, ?, ?>> nodeClass) {
        return node(nodeClass);
    }

    /**
     * 辅助方法：从节点类获取节点名称。
     * 默认使用类名。如果节点实现了自定义的 getName()，这里无法直接获取，
     * 但通常 Builder 是基于类结构的，所以使用类名是合理的约定。
     * 如果需要支持自定义名称，Builder 需要另一种方式来引用节点（比如直接用字符串名称）。
     */
    private String getNodeNameFromClass(Class<? extends DagNode<C, ?, ?>> nodeClass) {
        // 假设节点名称就是其简单类名，这与 DagNode.getName() 的默认实现一致
        // 并且 AbstractDagDefinition 在注册时也强制了名称唯一性（通常基于 getName()）
        // 如果节点覆盖了 getName() 返回非类名，这里需要调整或 Builder 使用字符串名称
        return nodeClass.getSimpleName();
    }

    private Class<? extends DagNode<C, ?, ?>> lastNodeAddedLinearly = null;

    public ChainBuilder<C> startLinear(Class<? extends DagNode<C, ?, ?>> nodeClass) {
        node(nodeClass); // 添加为无依赖节点
        lastNodeAddedLinearly = nodeClass;
        return this;
    }

    public ChainBuilder<C> then(Class<? extends DagNode<C, ?, ?>> nodeClass) {
        if (lastNodeAddedLinearly == null) {
            throw new IllegalStateException("Cannot use .then() without a preceding node in the linear chain setup (use startLinear first).");
        }
        node(nodeClass, lastNodeAddedLinearly); // 依赖上一个节点
        lastNodeAddedLinearly = nodeClass; // 更新最后一个节点
        return this;
    }

}
