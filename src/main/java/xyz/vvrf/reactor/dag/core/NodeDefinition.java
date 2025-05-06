package xyz.vvrf.reactor.dag.core;

import java.util.*;

/**
 * DAG 中节点实例的定义（不可变数据类）。
 * 包含实例名称、引用的节点类型 ID 和可选的节点特定配置。
 *
 * @author Refactored (注释更新)
 */
public final class NodeDefinition {
    private final String instanceName; // 节点实例在 DAG 中的唯一名称
    private final String nodeTypeId;   // 指向 NodeRegistry 中的 Key，代表节点的逻辑类型
    private final Map<String, Object> configuration; // 节点特定配置 (不可变)

    /**
     * 创建节点定义。
     *
     * @param instanceName  实例名称 (不能为空)
     * @param nodeTypeId    节点类型 ID (不能为空)
     * @param configuration 节点特定配置 (可以为 null 或空 Map)
     */
    public NodeDefinition(String instanceName, String nodeTypeId, Map<String, Object> configuration) {
        this.instanceName = Objects.requireNonNull(instanceName, "实例名称不能为空");
        this.nodeTypeId = Objects.requireNonNull(nodeTypeId, "节点类型 ID 不能为空");
        // 创建配置的不可变副本
        this.configuration = (configuration != null)
                ? Collections.unmodifiableMap(new HashMap<>(configuration))
                : Collections.emptyMap();
    }

    /**
     * 创建没有特定配置的节点定义。
     *
     * @param instanceName 实例名称 (不能为空)
     * @param nodeTypeId   节点类型 ID (不能为空)
     */
    public NodeDefinition(String instanceName, String nodeTypeId) {
        this(instanceName, nodeTypeId, null);
    }

    // --- Getters ---

    public String getInstanceName() {
        return instanceName;
    }

    public String getNodeTypeId() {
        return nodeTypeId;
    }

    /**
     * 获取节点特定的配置。
     * @return 不可变的配置 Map。
     */
    public Map<String, Object> getConfiguration() {
        return configuration;
    }

    /**
     * 辅助方法，用于安全地获取特定类型的配置值。
     *
     * @param <T>        期望的配置值类型
     * @param key          配置键
     * @param expectedType 期望的类型 Class 对象
     * @return 包含配置值的 Optional，如果键不存在、值为 null 或类型不匹配则为空。
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getConfig(String key, Class<T> expectedType) {
        return Optional.ofNullable(configuration.get(key))
                .filter(expectedType::isInstance)
                .map(expectedType::cast);
    }

    // --- equals, hashCode, toString ---

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeDefinition that = (NodeDefinition) o;
        return instanceName.equals(that.instanceName) &&
                nodeTypeId.equals(that.nodeTypeId) &&
                configuration.equals(that.configuration); // Map 的 equals 比较内容
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceName, nodeTypeId, configuration);
    }

    @Override
    public String toString() {
        return String.format("NodeDef[instance=%s, type=%s, config=%s]",
                instanceName, nodeTypeId, configuration);
    }
}
