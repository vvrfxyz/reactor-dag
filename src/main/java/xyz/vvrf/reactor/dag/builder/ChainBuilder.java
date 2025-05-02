// [file name]: ChainBuilder.java
package xyz.vvrf.reactor.dag.builder;

import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.DagNodeDefinition;
import xyz.vvrf.reactor.dag.core.NodeLogic;
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition; // 依赖抽象实现添加节点定义
import reactor.util.retry.Retry; // 引入 Retry
import java.time.Duration; // 引入 Duration
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate; // 引入 BiPredicate
import xyz.vvrf.reactor.dag.core.DependencyAccessor; // 引入 DependencyAccessor

/**
 * DAG 链式构建器。
 * 用于以编程方式定义 DAG 结构，包括节点逻辑、名称、依赖关系和可选配置。
 *
 * @param <C> 上下文类型
 */
@Slf4j
public class ChainBuilder<C> {

    private final AbstractDagDefinition<C> dagDefinition;
    // 存储临时的节点配置信息，Key 是节点名称
    private final Map<String, NodeConfig<C>> nodeConfigs = new ConcurrentHashMap<>();

    // 内部类，用于暂存节点配置
    private static class NodeConfig<C> {
        final String nodeName;
        final NodeLogic<C, ?> logic;
        final List<String> dependencyNames = new ArrayList<>();
        Retry retrySpec = null;
        Duration timeout = null;
        BiPredicate<C, DependencyAccessor<C>> shouldExecutePredicate = null;

        NodeConfig(String nodeName, NodeLogic<C, ?> logic) {
            this.nodeName = nodeName;
            this.logic = logic;
        }
    }

    /**
     * 创建 ChainBuilder 实例。
     *
     * @param dagDefinition 正在配置的 DagDefinition 实例 (必须是 AbstractDagDefinition 的子类)。
     */
    public ChainBuilder(AbstractDagDefinition<C> dagDefinition) {
        this.dagDefinition = Objects.requireNonNull(dagDefinition, "DagDefinition 不能为空");
        log.info("[{}] DAG '{}': ChainBuilder 已创建。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());
    }

    /**
     * 定义一个 DAG 节点实例。
     *
     * @param nodeName           节点的唯一名称。
     * @param logic              该节点使用的 NodeLogic 实现。
     * @param dependencyNodeNames 它所依赖的节点的名称数组。如果为空或不传，表示此节点无依赖。
     * @return 当前 ChainBuilder 实例，支持链式调用。
     * @throws NullPointerException 如果 nodeName 或 logic 为 null。
     * @throws IllegalArgumentException 如果 nodeName 为空。
     */
    public ChainBuilder<C> node(String nodeName, NodeLogic<C, ?> logic, String... dependencyNodeNames) {
        Objects.requireNonNull(nodeName, "节点名称不能为空");
        Objects.requireNonNull(logic, "NodeLogic 实现不能为空 for node " + nodeName);
        if (nodeName.trim().isEmpty()) {
            throw new IllegalArgumentException("节点名称不能为空白");
        }

        NodeConfig<C> config = nodeConfigs.computeIfAbsent(nodeName, k -> new NodeConfig<>(k, logic));

        // 清空旧依赖，设置新依赖
        config.dependencyNames.clear();
        if (dependencyNodeNames != null) {
            for(String depName : dependencyNodeNames) {
                Objects.requireNonNull(depName, "依赖节点名称不能为空 for node " + nodeName);
                if (depName.trim().isEmpty()) {
                    throw new IllegalArgumentException("依赖节点名称不能为空白 for node " + nodeName);
                }
                if (depName.equals(nodeName)) {
                    throw new IllegalArgumentException(String.format("节点 '%s' 不能依赖自身", nodeName));
                }
                config.dependencyNames.add(depName);
            }
        }

        log.debug("[{}] DAG '{}': Builder 定义节点 '{}' (逻辑: {}) 依赖于: {}",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                nodeName, logic.getLogicIdentifier(), config.dependencyNames);

        return this;
    }

    /**
     * 为最近定义的节点设置自定义重试策略。
     * 会覆盖 NodeLogic 的默认重试策略。
     *
     * @param nodeName  要配置的节点名称。
     * @param retrySpec Reactor 的 Retry 规范。
     * @return 当前 ChainBuilder 实例。
     * @throws IllegalStateException 如果节点尚未通过 node() 方法定义。
     */
    public ChainBuilder<C> withRetry(String nodeName, Retry retrySpec) {
        NodeConfig<C> config = getNodeConfigOrThrow(nodeName);
        config.retrySpec = retrySpec; // 允许为 null 以清除设置
        log.debug("[{}] DAG '{}': Builder 为节点 '{}' 配置了自定义 Retry 策略。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(), nodeName);
        return this;
    }

    /**
     * 为最近定义的节点设置自定义执行超时。
     * 会覆盖 NodeLogic 的默认超时。
     *
     * @param nodeName 要配置的节点名称。
     * @param timeout  超时时间。
     * @return 当前 ChainBuilder 实例。
     * @throws IllegalStateException 如果节点尚未通过 node() 方法定义。
     * @throws NullPointerException 如果 timeout 为 null。
     */
    public ChainBuilder<C> withTimeout(String nodeName, Duration timeout) {
        NodeConfig<C> config = getNodeConfigOrThrow(nodeName);
        config.timeout = Objects.requireNonNull(timeout, "Timeout 不能为空 for node " + nodeName);
        log.debug("[{}] DAG '{}': Builder 为节点 '{}' 配置了自定义 Timeout: {}",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(), nodeName, timeout);
        return this;
    }

    /**
     * 为最近定义的节点设置自定义的执行条件。
     * 会覆盖 NodeLogic 的默认 shouldExecute 行为。
     *
     * @param nodeName    要配置的节点名称。
     * @param predicate   一个 BiPredicate，接收 Context 和 DependencyAccessor，返回 boolean。
     * @return 当前 ChainBuilder 实例。
     * @throws IllegalStateException 如果节点尚未通过 node() 方法定义。
     * @throws NullPointerException 如果 predicate 为 null。
     */
    public ChainBuilder<C> when(String nodeName, BiPredicate<C, DependencyAccessor<C>> predicate) {
        NodeConfig<C> config = getNodeConfigOrThrow(nodeName);
        config.shouldExecutePredicate = Objects.requireNonNull(predicate, "Predicate 不能为空 for node " + nodeName);
        log.debug("[{}] DAG '{}': Builder 为节点 '{}' 配置了自定义执行条件 (when)。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(), nodeName);
        return this;
    }

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
     *
     * @throws IllegalStateException 如果在应用配置时发生错误（例如依赖的节点未在 Builder 中定义）。
     */
    public void applyToDefinition() {
        log.info("[{}] DAG '{}': 开始将 ChainBuilder 定义的节点配置应用到 DagDefinition...",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());

        // 验证所有依赖的节点是否都已在 Builder 中定义
        validateAllDependenciesExist();

        int appliedCount = 0;
        for (NodeConfig<C> config : nodeConfigs.values()) {
            try {
                // 创建 DagNodeDefinition 实例
                DagNodeDefinition<C, ?> nodeDefinition = new DagNodeDefinition<>(
                        config.nodeName,
                        config.logic,
                        Collections.unmodifiableList(new ArrayList<>(config.dependencyNames)), // 传递不可变列表
                        config.retrySpec,
                        config.timeout,
                        config.shouldExecutePredicate
                );
                // 调用 DagDefinition 的方法来添加节点定义
                dagDefinition.addNodeDefinition(nodeDefinition);
                appliedCount++;
            } catch (Exception e) { // 捕获 addNodeDefinition 可能抛出的异常 (如重名)
                log.error("[{}] DAG '{}': 应用节点 '{}' 的配置时失败: {}",
                        dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                        config.nodeName, e.getMessage(), e);
                // 重新抛出，指示应用过程失败
                throw new IllegalStateException("Failed to apply configuration for node '" + config.nodeName + "'", e);
            }
        }
        log.info("[{}] DAG '{}': 成功应用了 {} 个节点的配置到 DagDefinition。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(), appliedCount);
    }

    /**
     * 验证所有节点配置中声明的依赖项是否也在本 Builder 中定义了。
     */
    private void validateAllDependenciesExist() {
        log.debug("[{}] DAG '{}': Builder 验证所有依赖节点是否存在于定义中...",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());
        for (NodeConfig<C> config : nodeConfigs.values()) {
            for (String depName : config.dependencyNames) {
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
