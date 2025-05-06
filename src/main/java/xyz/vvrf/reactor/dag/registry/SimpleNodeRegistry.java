// file: registry/SimpleNodeRegistry.java
package xyz.vvrf.reactor.dag.registry;

import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.InputSlot;
import xyz.vvrf.reactor.dag.core.OutputSlot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * NodeRegistry 的简单内存实现。
 *
 * @param <C> 上下文类型
 * @author Refactored
 */
@Slf4j
public class SimpleNodeRegistry<C> implements NodeRegistry<C> {

    private final Class<C> contextType; // 存储上下文类型
    private final Map<String, Supplier<? extends DagNode<C, ?>>> factoryMap = new ConcurrentHashMap<>();
    private final Map<String, NodeMetadata> metadataMap = new ConcurrentHashMap<>();

    /**
     * 创建 SimpleNodeRegistry 实例。
     * @param contextType 此注册表关联的上下文类型，不能为 null。
     */
    public SimpleNodeRegistry(Class<C> contextType) {
        this.contextType = Objects.requireNonNull(contextType, "Context type cannot be null for SimpleNodeRegistry");
    }

    @Override
    public Class<C> getContextType() {
        return contextType;
    }

    @Override
    public void register(String nodeTypeId, Supplier<? extends DagNode<C, ?>> factory) {
        Objects.requireNonNull(nodeTypeId, "Node type ID cannot be null");
        Objects.requireNonNull(factory, "Node factory cannot be null");
        if (factoryMap.containsKey(nodeTypeId)) {
            throw new IllegalArgumentException("Node type ID '" + nodeTypeId + "' is already registered for context " + contextType.getSimpleName());
        }
        // 预先获取一次实例以提取元数据
        DagNode<C, ?> sampleInstance;
        try {
            sampleInstance = factory.get();
            if (sampleInstance == null) {
                throw new IllegalArgumentException("Factory for Node type ID '" + nodeTypeId + "' returned null.");
            }
        } catch (Exception e) {
            log.error("Failed to get sample instance from factory for node type '{}'", nodeTypeId, e);
            throw new IllegalArgumentException("Failed to instantiate node from factory for metadata extraction: " + nodeTypeId, e);
        }

        NodeMetadata metadata = extractMetadata(nodeTypeId, sampleInstance);

        factoryMap.put(nodeTypeId, factory);
        metadataMap.put(nodeTypeId, metadata);
        log.info("Registered node type '{}' using factory (Impl: {}, Context: {})",
                nodeTypeId, sampleInstance.getClass().getName(), contextType.getSimpleName());
    }

    @Override
    public void register(String nodeTypeId, DagNode<C, ?> prototype) {
        Objects.requireNonNull(nodeTypeId, "Node type ID cannot be null");
        Objects.requireNonNull(prototype, "Node prototype cannot be null");
        if (factoryMap.containsKey(nodeTypeId)) {
            throw new IllegalArgumentException("Node type ID '" + nodeTypeId + "' is already registered for context " + contextType.getSimpleName());
        }
        NodeMetadata metadata = extractMetadata(nodeTypeId, prototype);

        // 将原型包装成 Supplier
        factoryMap.put(nodeTypeId, () -> prototype);
        metadataMap.put(nodeTypeId, metadata);
        log.info("Registered node type '{}' using prototype (Impl: {}, Context: {})",
                nodeTypeId, prototype.getClass().getName(), contextType.getSimpleName());
    }

    @Override
    public Optional<DagNode<C, ?>> getNodeInstance(String nodeTypeId) {
        Supplier<? extends DagNode<C, ?>> factory = factoryMap.get(nodeTypeId);
        return Optional.ofNullable(factory).map(Supplier::get);
    }

    @Override
    public Optional<NodeMetadata> getNodeMetadata(String nodeTypeId) {
        return Optional.ofNullable(metadataMap.get(nodeTypeId));
    }

    private NodeMetadata extractMetadata(String nodeTypeId, DagNode<C, ?> nodeInstance) {
        final Set<InputSlot<?>> inputSlots;
        final OutputSlot<?> primaryOutputSlot;
        final Set<OutputSlot<?>> additionalOutputSlots;

        try {
            inputSlots = nodeInstance.getInputSlots();
            primaryOutputSlot = nodeInstance.getOutputSlot();
            additionalOutputSlots = nodeInstance.getAdditionalOutputSlots();

            Objects.requireNonNull(inputSlots, "getInputSlots() cannot return null for node type " + nodeTypeId);
            Objects.requireNonNull(primaryOutputSlot, "getOutputSlot() cannot return null for node type " + nodeTypeId);
            Objects.requireNonNull(additionalOutputSlots, "getAdditionalOutputSlots() cannot return null for node type " + nodeTypeId);

        } catch (Exception e) {
            log.error("Error extracting metadata from node instance of type '{}' (Impl: {})",
                    nodeTypeId, nodeInstance.getClass().getName(), e);
            throw new IllegalArgumentException("Failed to extract metadata from node instance: " + nodeTypeId, e);
        }


        // 可以在这里添加更多验证，例如检查 Slot ID 的唯一性等

        return new NodeMetadata() {
            @Override public String getTypeId() { return nodeTypeId; }
            @Override public Set<InputSlot<?>> getInputSlots() { return Collections.unmodifiableSet(inputSlots); } // 返回不可变集合
            @Override public OutputSlot<?> getPrimaryOutputSlot() { return primaryOutputSlot; }
            @Override public Set<OutputSlot<?>> getAdditionalOutputSlots() { return Collections.unmodifiableSet(additionalOutputSlots); } // 返回不可变集合
        };
    }
}
