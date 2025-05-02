// [文件名称]: ChainBuilder.java
package xyz.vvrf.reactor.dag.builder;

import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.*; // 引入 EdgeDefinition, NodeResult
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition;
import reactor.util.retry.Retry;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;

/**
 * DAG 链式构建器。
 * 用于以编程方式定义 DAG 结构，包括节点逻辑、名称、边（依赖及条件）和可选配置。
 *
 * @param <C> 上下文类型
 */
@Slf4j
public class ChainBuilder<C> {

    private final AbstractDagDefinition<C> dagDefinition;
    // 存储临时的节点配置信息，Key 是节点名称
    private final Map<String, NodeConfig<C>> nodeConfigs = new ConcurrentHashMap<>();
    // 记录最后通过 node() 或 dependsOn() 操作的节点名，用于链式配置边条件
    private String lastConfiguredNodeName = null;
    private String lastAddedDependencyName = null;


    // 内部类，用于暂存节点配置
    private static class NodeConfig<C> {
        final String nodeName;
        final NodeLogic<C, ?> logic;
        // 存储临时的边信息，Key 是依赖节点名，Value 是条件 Predicate (null 表示默认)
        final Map<String, BiPredicate<C, NodeResult<C, ?>>> edges = new LinkedHashMap<>(); // 使用 LinkedHashMap 保持添加顺序
        Retry retrySpec = null;
        Duration timeout = null;
        // 移除了 shouldExecutePredicate

        NodeConfig(String nodeName, NodeLogic<C, ?> logic) {
            this.nodeName = nodeName;
            this.logic = logic;
        }
    }

    /**
     * 创建 ChainBuilder 实例。
     */
    public ChainBuilder(AbstractDagDefinition<C> dagDefinition) {
        this.dagDefinition = Objects.requireNonNull(dagDefinition, "DagDefinition 不能为空");
        log.info("[{}] DAG '{}': ChainBuilder 已创建。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());
    }

    /**
     * 定义一个 DAG 节点实例。
     * 如果需要定义依赖，请在此方法后链式调用 .dependsOn()。
     *
     * @param nodeName 节点的唯一名称。
     * @param logic    该节点使用的 NodeLogic 实现。
     * @return 当前 ChainBuilder 实例，支持链式调用。
     */
    public ChainBuilder<C> node(String nodeName, NodeLogic<C, ?> logic) {
        Objects.requireNonNull(nodeName, "节点名称不能为空");
        Objects.requireNonNull(logic, "NodeLogic 实现不能为空 for node " + nodeName);
        if (nodeName.trim().isEmpty()) {
            throw new IllegalArgumentException("节点名称不能为空白");
        }

        if (nodeConfigs.containsKey(nodeName)) {
            // 允许重新定义节点，但会覆盖之前的配置（包括边）
            log.warn("[{}] DAG '{}': Builder 重新定义节点 '{}'，旧配置将被覆盖。",
                    dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(), nodeName);
        }

        NodeConfig<C> config = new NodeConfig<>(nodeName, logic);
        nodeConfigs.put(nodeName, config);
        this.lastConfiguredNodeName = nodeName; // 记录当前配置的节点
        this.lastAddedDependencyName = null; // 重置最后添加的依赖

        log.debug("[{}] DAG '{}': Builder 定义节点 '{}' (逻辑: {})",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                nodeName, logic.getLogicIdentifier());

        return this;
    }

    /**
     * 为最近通过 node() 定义的节点添加一个依赖边。
     * 默认情况下，这条边使用默认条件（依赖节点成功）。
     * 可以链式调用 .withCondition() 来设置自定义条件。
     *
     * @param dependencyNodeName 依赖的源节点名称。
     * @return 当前 ChainBuilder 实例。
     * @throws IllegalStateException 如果尚未调用 node() 定义节点，或依赖自身。
     * @throws NullPointerException 如果 dependencyNodeName 为 null。
     * @throws IllegalArgumentException 如果 dependencyNodeName 为空。
     */
    public ChainBuilder<C> dependsOn(String dependencyNodeName) {
        Objects.requireNonNull(dependencyNodeName, "依赖节点名称不能为空");
        if (dependencyNodeName.trim().isEmpty()) {
            throw new IllegalArgumentException("依赖节点名称不能为空白");
        }
        if (this.lastConfiguredNodeName == null) {
            throw new IllegalStateException("必须先调用 node() 定义节点，然后才能调用 dependsOn()");
        }
        if (dependencyNodeName.equals(this.lastConfiguredNodeName)) {
            throw new IllegalArgumentException(String.format("节点 '%s' 不能依赖自身", this.lastConfiguredNodeName));
        }

        NodeConfig<C> config = getNodeConfigOrThrow(this.lastConfiguredNodeName);
        // 添加边，使用 null 代表默认条件，后续 withCondition 可以覆盖
        config.edges.put(dependencyNodeName, null);
        this.lastAddedDependencyName = dependencyNodeName; // 记录最后添加的依赖

        log.debug("[{}] DAG '{}': Builder 为节点 '{}' 添加依赖边 -> '{}' (默认条件)",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                dependencyNodeName, this.lastConfiguredNodeName);

        return this;
    }

    /**
     * 为最近通过 dependsOn() 添加的依赖边设置自定义条件。
     *
     * @param condition 一个 BiPredicate，接收 Context 和 依赖节点的 NodeResult，返回 boolean。
     * @return 当前 ChainBuilder 实例。
     * @throws IllegalStateException 如果尚未调用 dependsOn() 添加依赖边。
     * @throws NullPointerException 如果 condition 为 null。
     */
    public ChainBuilder<C> withCondition(BiPredicate<C, NodeResult<C, ?>> condition) {
        Objects.requireNonNull(condition, "边的条件 Predicate 不能为空");
        if (this.lastConfiguredNodeName == null || this.lastAddedDependencyName == null) {
            throw new IllegalStateException("必须先调用 node().dependsOn() 添加依赖边，然后才能调用 withCondition()");
        }

        NodeConfig<C> config = getNodeConfigOrThrow(this.lastConfiguredNodeName);
        // 检查依赖是否存在于 map 中 (理论上 dependsOn 已添加)
        if (!config.edges.containsKey(this.lastAddedDependencyName)) {
            // 这种情况理论上不应发生
            throw new IllegalStateException(String.format("内部错误：尝试为不存在的依赖 '%s' -> '%s' 设置条件",
                    this.lastAddedDependencyName, this.lastConfiguredNodeName));
        }
        // 更新最后添加的边的条件
        config.edges.put(this.lastAddedDependencyName, condition);

        log.debug("[{}] DAG '{}': Builder 为边 '{}' -> '{}' 设置了自定义条件。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                this.lastAddedDependencyName, this.lastConfiguredNodeName);

        // 清除 lastAddedDependencyName，防止重复为同一条边设置条件或误操作
        // this.lastAddedDependencyName = null; // 或者不清，允许连续调用 withCondition 覆盖？目前选择不清，允许覆盖。

        return this;
    }


    /**
     * 为指定名称的节点设置自定义重试策略。
     * 会覆盖 NodeLogic 的默认重试策略。
     *
     * @param nodeName  要配置的节点名称。
     * @param retrySpec Reactor 的 Retry 规范。
     * @return 当前 ChainBuilder 实例。
     */
    public ChainBuilder<C> withRetry(String nodeName, Retry retrySpec) {
        NodeConfig<C> config = getNodeConfigOrThrow(nodeName);
        config.retrySpec = retrySpec; // 允许为 null 以清除设置
        log.debug("[{}] DAG '{}': Builder 为节点 '{}' 配置了自定义 Retry 策略。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(), nodeName);
        return this;
    }

    /**
     * 为指定名称的节点设置自定义执行超时。
     * 会覆盖 NodeLogic 的默认超时。
     *
     * @param nodeName 要配置的节点名称。
     * @param timeout  超时时间。
     * @return 当前 ChainBuilder 实例。
     */
    public ChainBuilder<C> withTimeout(String nodeName, Duration timeout) {
        NodeConfig<C> config = getNodeConfigOrThrow(nodeName);
        config.timeout = Objects.requireNonNull(timeout, "Timeout 不能为空 for node " + nodeName);
        log.debug("[{}] DAG '{}': Builder 为节点 '{}' 配置了自定义 Timeout: {}",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(), nodeName, timeout);
        return this;
    }

    // 移除了 when() 方法，因为条件现在在边上定义
    // public ChainBuilder<C> when(String nodeName, BiPredicate<C, Map<String, NodeResult<C, ?>>> predicate) { ... }


    /**
     * 获取节点配置，如果不存在则抛出异常。
     */
    private NodeConfig<C> getNodeConfigOrThrow(String nodeName) {
        NodeConfig<C> config = nodeConfigs.get(nodeName);
        if (config == null) {
            throw new IllegalStateException(String.format("节点 '%s' 尚未通过 node() 方法定义，无法配置其属性。", nodeName));
        }
        return config;
    }

    /**
     * 将通过此 Builder 定义的所有节点及其配置应用到关联的 DagDefinition 实例。
     * 调用此方法后，通常应接着调用 {@link AbstractDagDefinition#initialize()}。
     */
    public void applyToDefinition() {
        log.info("[{}] DAG '{}': 开始将 ChainBuilder 定义的节点配置应用到 DagDefinition...",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());

        // 验证所有依赖的节点是否都已在 Builder 中定义
        validateAllDependenciesExist();

        int appliedCount = 0;
        for (NodeConfig<C> config : nodeConfigs.values()) {
            try {
                // 创建 EdgeDefinition 列表
                List<EdgeDefinition<C>> edgeDefinitions = new ArrayList<>();
                for (Map.Entry<String, BiPredicate<C, NodeResult<C, ?>>> entry : config.edges.entrySet()) {
                    edgeDefinitions.add(new EdgeDefinition<>(entry.getKey(), entry.getValue()));
                }

                // 创建 DagNodeDefinition 实例
                // 注意：泛型 T 在这里无法直接确定，使用 ? 通配符
                DagNodeDefinition<C, ?> nodeDefinition = new DagNodeDefinition<>(
                        config.nodeName,
                        config.logic,
                        edgeDefinitions, // 传递构建好的边定义列表
                        config.retrySpec,
                        config.timeout
                        // 移除了 shouldExecutePredicate
                );
                // 调用 DagDefinition 的方法来添加节点定义
                dagDefinition.addNodeDefinition(nodeDefinition);
                appliedCount++;
            } catch (Exception e) {
                log.error("[{}] DAG '{}': 应用节点 '{}' 的配置时失败: {}",
                        dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                        config.nodeName, e.getMessage(), e);
                throw new IllegalStateException("应用节点 '" + config.nodeName + "' 的配置失败", e);
            }
        }
        log.info("[{}] DAG '{}': 成功应用了 {} 个节点的配置到 DagDefinition。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(), appliedCount);

        // 清理 builder 状态，以便重用（如果需要）
        // nodeConfigs.clear();
        // lastConfiguredNodeName = null;
        // lastAddedDependencyName = null;
    }

    /**
     * 验证所有节点配置中声明的依赖项是否也在本 Builder 中定义了。
     */
    private void validateAllDependenciesExist() {
        log.debug("[{}] DAG '{}': Builder 验证所有依赖节点是否存在于定义中...",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());
        for (NodeConfig<C> config : nodeConfigs.values()) {
            // 遍历所有定义的边
            for (String depName : config.edges.keySet()) {
                if (!nodeConfigs.containsKey(depName)) {
                    String errorMsg = String.format("节点 '%s' 依赖的节点 '%s' 未在本 ChainBuilder 中通过 node() 方法定义。",
                            config.nodeName, depName);
                    log.error("[{}] DAG '{}': {}", dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(), errorMsg);
                    throw new IllegalStateException(errorMsg);
                }
            }
        }
        log.debug("[{}] DAG '{}': Builder 依赖节点存在性验证通过。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());
    }

    /**
     * 定义一个没有依赖的起始节点。
     * 等同于调用 node(nodeName, logic)。
     *
     * @param nodeName 起点节点名称。
     * @param logic    起点节点使用的 NodeLogic 实现。
     * @return 当前 ChainBuilder 实例。
     */
    public ChainBuilder<C> startWith(String nodeName, NodeLogic<C, ?> logic) {
        return node(nodeName, logic);
    }
}
