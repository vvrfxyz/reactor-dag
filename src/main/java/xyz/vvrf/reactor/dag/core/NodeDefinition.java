package xyz.vvrf.reactor.dag.core;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * DAG 中节点实例的定义（纯数据）。
 * 包含实例名称、引用的节点类型 ID 和可选配置。
 *
 * @author Refactored
 */
public final class NodeDefinition {
    private final String instanceName;
    private final String nodeTypeId; // 指向 NodeRegistry 中的 Key
    private final Map<String, Object> configuration; // 节点特定配置

    public NodeDefinition(String instanceName, String nodeTypeId, Map<String, Object> configuration) {
        this.instanceName = Objects.requireNonNull(instanceName, "实例名称不能为空");
        this.nodeTypeId = Objects.requireNonNull(nodeTypeId, "节点类型 ID 不能为空");
        this.configuration = configuration != null ? Collections.unmodifiableMap(configuration) : Collections.emptyMap();
    }

    public NodeDefinition(String instanceName, String nodeTypeId) {
        this(instanceName, nodeTypeId, null);
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getNodeTypeId() {
        return nodeTypeId;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }

    // 可选的辅助方法，用于获取特定类型的配置值
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getConfig(String key, Class<T> type) {
        return Optional.ofNullable(configuration.get(key))
                .filter(type::isInstance)
                .map(type::cast);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeDefinition that = (NodeDefinition) o;
        return instanceName.equals(that.instanceName) && nodeTypeId.equals(that.nodeTypeId) && configuration.equals(that.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceName, nodeTypeId, configuration);
    }

    @Override
    public String toString() {
        return "NodeDefinition{" +
                "instanceName='" + instanceName + '\'' +
                ", nodeTypeId='" + nodeTypeId + '\'' +
                ", configuration=" + configuration +
                '}';
    }
}