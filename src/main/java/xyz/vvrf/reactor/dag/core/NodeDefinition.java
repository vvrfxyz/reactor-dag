// 文件名: core/NodeDefinition.java (已修改)
package xyz.vvrf.reactor.dag.core;

import java.util.*;

/**
 * 定义 DAG 中的一个节点实例（数据类）。
 * 包含实例名称、引用的节点类型 ID 以及可选的实例特定配置。
 * 由构建器创建的实例在配置方面初始是可变的，
 * 但在构建最终的 DagDefinition 时变为不可变。
 *
 * @author Refactored
 */
public final class NodeDefinition {
    private final String instanceName; // DAG 内唯一名称
    private final String nodeTypeId;   // NodeRegistry 中的 Key，代表节点的逻辑类型
    private Map<String, Object> configuration; // 节点特定配置（初始可变，然后不可变）
    private boolean immutable = false; // 标记以防止构建后修改

    /**
     * 创建节点定义（由构建器内部使用）。
     *
     * @param instanceName  实例名称 (非空)。
     * @param nodeTypeId    节点类型 ID (非空)。
     * @param configuration 初始配置 Map (可为 null 或空，将被复制)。
     */
    public NodeDefinition(String instanceName, String nodeTypeId, Map<String, Object> configuration) {
        this.instanceName = Objects.requireNonNull(instanceName, "实例名称不能为空");
        this.nodeTypeId = Objects.requireNonNull(nodeTypeId, "节点类型 ID 不能为空");
        // 创建一个可变副本供构建器修改
        this.configuration = (configuration != null)
                ? new HashMap<>(configuration)
                : new HashMap<>();
    }

    // --- Getters ---

    public String getInstanceName() {
        return instanceName;
    }

    public String getNodeTypeId() {
        return nodeTypeId;
    }

    /**
     * 获取节点特定的配置 Map。
     * 返回的 Map 是不可变的。
     * @return 配置 Map 的不可变视图。
     */
    public Map<String, Object> getConfiguration() {
        // 确保内部 Map 在返回前是不可变的
        if (!immutable) {
            makeImmutable(); // 理想情况下应由 builder 的 build() 方法调用
        }
        return configuration; // 返回（现在）不可变的 Map
    }

    /**
     * 安全获取特定类型配置值的辅助方法。
     *
     * @param <T>          期望的配置值类型。
     * @param key          配置键。
     * @param expectedType 期望的类型 Class 对象。
     * @return 包含配置值的 Optional，如果键不存在、值为 null 或类型不匹配则为空。
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getConfig(String key, Class<T> expectedType) {
        // 使用公共 getter 获取不可变 Map
        return Optional.ofNullable(getConfiguration().get(key))
                .filter(expectedType::isInstance)
                .map(expectedType::cast);
    }

    // --- 包私有方法供 Builder 使用 ---

    /**
     * 获取内部可变的配置 Map。仅供 DagDefinitionBuilder 使用。
     * @throws IllegalStateException 如果定义已被设为不可变。
     */
    public Map<String, Object> getConfigurationInternal() {
        if (immutable) {
            throw new IllegalStateException("实例 '" + instanceName + "' 的 NodeDefinition 已不可变，无法进一步配置。");
        }
        return configuration;
    }

    /**
     * 使内部配置 Map 不可变。由 DagDefinitionBuilder.build() 调用。
     * @return this NodeDefinition 实例。
     */
    public NodeDefinition makeImmutable() {
        if (!immutable) {
            this.configuration = Collections.unmodifiableMap(this.configuration);
            this.immutable = true;
        }
        return this;
    }


    // --- equals, hashCode, toString ---

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeDefinition that = (NodeDefinition) o;
        // 基于 instanceName, typeId 和配置 Map 的内容进行比较
        return instanceName.equals(that.instanceName) &&
                nodeTypeId.equals(that.nodeTypeId) &&
                getConfiguration().equals(that.getConfiguration()); // 使用 getter 确保比较的是不可变 Map
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceName, nodeTypeId, getConfiguration()); // 使用 getter
    }

    @Override
    public String toString() {
        return String.format("NodeDef[instance=%s, type=%s, config=%s]",
                instanceName, nodeTypeId, getConfiguration()); // 使用 getter
    }
}
