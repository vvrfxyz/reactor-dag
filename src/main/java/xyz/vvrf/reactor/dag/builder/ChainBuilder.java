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

    private final AbstractDagDefinition<C> dagDefinition; // 需要引用来查找节点信息和添加显式依赖
    private final Map<String, List<DependencyDescriptor>> explicitDependenciesMap;

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
     * @param nodeName           要添加或配置依赖的目标节点的名称 (必须已在 DagDefinition 注册)。
     * @param dependencyNodeNames 它所依赖的节点的名称数组。如果为空或不传，表示此节点在此链中无显式依赖。
     * @return 当前 ChainBuilder 实例，支持链式调用。
     * @throws IllegalArgumentException 如果目标节点或依赖的节点未在 DagDefinition 中注册，
     *                                或无法获取其 PayloadType。
     * @throws NullPointerException 如果节点名称为 null。
     */
    public ChainBuilder<C> node(String nodeName, String... dependencyNodeNames) {
        Objects.requireNonNull(nodeName, "目标节点名称不能为空");

        // 确认目标节点已注册 (使用 nodeName)
        if (!dagDefinition.getNodeAnyType(nodeName).isPresent()) {
            String errorMsg = String.format("目标节点 '%s' 未在 DagDefinition '%s' 中注册。",
                    nodeName, dagDefinition.getDagName());
            log.error("[{}] {}", dagDefinition.getContextType().getSimpleName(), errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        List<DependencyDescriptor> dependencies = new ArrayList<>();
        if (dependencyNodeNames != null) {
            for (String depNodeName : dependencyNodeNames) {
                Objects.requireNonNull(depNodeName, "依赖节点名称不能为空");

                // 从 DagDefinition 中查找依赖节点实例以获取其 PayloadType (使用 depNodeName)
                Optional<DagNode<C, ?, ?>> depNodeOpt = dagDefinition.getNodeAnyType(depNodeName);
                if (!depNodeOpt.isPresent()) {
                    String errorMsg = String.format("节点 '%s' 的依赖节点 '%s' 未在 DagDefinition '%s' 中注册。",
                            nodeName, depNodeName, dagDefinition.getDagName());
                    log.error("[{}] {}", dagDefinition.getContextType().getSimpleName(), errorMsg);
                    throw new IllegalArgumentException(errorMsg);
                }

                DagNode<C, ?, ?> depNode = depNodeOpt.get();
                Class<?> requiredType = depNode.getPayloadType();
                if (requiredType == null) {
                    String errorMsg = String.format("节点 '%s' 的依赖节点 '%s' (类: %s) 未提供有效的 PayloadType。",
                            nodeName, depNodeName, depNode.getClass().getName()); // 日志中仍可包含类名
                    log.error("[{}] {}", dagDefinition.getContextType().getSimpleName(), errorMsg);
                    throw new IllegalArgumentException(errorMsg);
                }

                dependencies.add(new DependencyDescriptor(depNodeName, requiredType));
            }
        }

        log.debug("[{}] DAG '{}': Builder 定义节点 '{}' 依赖于: {}",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                nodeName, dependencies);
        // *** 使用 nodeName 作为 Key ***
        explicitDependenciesMap.put(nodeName, dependencies);
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
        for (Map.Entry<String, List<DependencyDescriptor>> entry : explicitDependenciesMap.entrySet()) {
            String nodeName = entry.getKey();

            if (dagDefinition.getNodeAnyType(nodeName).isPresent()) {
                finalDependencies.put(nodeName, entry.getValue());
            } else {
                log.warn("[{}] DAG '{}': 在构建最终依赖映射时，节点 '{}' 已不再注册，其显式依赖将被忽略。",
                        dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                        nodeName);
            }
        }
        log.info("[{}] DAG '{}': ChainBuilder 构建完成，生成了 {} 个节点的显式依赖配置。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                finalDependencies.size());
        return finalDependencies;
    }

    /**
     * 添加一个没有显式依赖的节点（例如，图的起点）。
     * 等同于调用 node(nodeName)。
     *
     * @param nodeName 起点节点名称。
     * @return 当前 ChainBuilder 实例。
     */
    public ChainBuilder<C> startWith(String nodeName) {
        return node(nodeName);
    }

}
