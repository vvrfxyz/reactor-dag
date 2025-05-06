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
 * 线程安全。
 *
 * @param <C> 上下文类型
 * @author Refactored (注释更新)
 */
@Slf4j
public class SimpleNodeRegistry<C> implements NodeRegistry<C> {

    private final Class<C> contextType; // 存储上下文类型
    // 存储节点工厂或原型包装成的工厂
    private final Map<String, Supplier<? extends DagNode<C, ?>>> factoryMap = new ConcurrentHashMap<>();
    // 缓存节点元数据
    private final Map<String, NodeMetadata> metadataMap = new ConcurrentHashMap<>();

    /**
     * 创建 SimpleNodeRegistry 实例。
     * @param contextType 此注册表关联的上下文类型 (不能为空)。
     */
    public SimpleNodeRegistry(Class<C> contextType) {
        this.contextType = Objects.requireNonNull(contextType, "上下文类型不能为空");
        log.info("SimpleNodeRegistry 已创建，关联上下文类型: {}", contextType.getSimpleName());
    }

    @Override
    public Class<C> getContextType() {
        return contextType;
    }

    @Override
    public void register(String nodeTypeId, Supplier<? extends DagNode<C, ?>> factory) {
        Objects.requireNonNull(nodeTypeId, "节点类型 ID 不能为空");
        Objects.requireNonNull(factory, "节点工厂不能为空");

        // 尝试原子性地放入，如果已存在则抛异常
        if (factoryMap.putIfAbsent(nodeTypeId, factory) != null) {
            throw new IllegalArgumentException(String.format("节点类型 ID '%s' 在上下文 '%s' 的注册表中已存在。",
                    nodeTypeId, contextType.getSimpleName()));
        }

        // 预先获取一次实例以提取元数据并进行校验
        DagNode<C, ?> sampleInstance;
        try {
            sampleInstance = factory.get();
            if (sampleInstance == null) {
                // 从 map 中移除无效的注册
                factoryMap.remove(nodeTypeId);
                throw new IllegalArgumentException(String.format("节点类型 '%s' 的工厂返回了 null 实例。", nodeTypeId));
            }
        } catch (Exception e) {
            // 从 map 中移除无效的注册
            factoryMap.remove(nodeTypeId);
            log.error("从工厂获取节点类型 '{}' 的示例实例失败。", nodeTypeId, e);
            throw new IllegalArgumentException("无法从工厂实例化节点以提取元数据: " + nodeTypeId, e);
        }

        // 提取并缓存元数据
        NodeMetadata metadata = extractAndCacheMetadata(nodeTypeId, sampleInstance);

        log.info("上下文 '{}': 已注册节点类型 '{}' (使用工厂, 实现: {})",
                contextType.getSimpleName(), nodeTypeId, sampleInstance.getClass().getName());
    }

    @Override
    public void register(String nodeTypeId, DagNode<C, ?> prototype) {
        Objects.requireNonNull(nodeTypeId, "节点类型 ID 不能为空");
        Objects.requireNonNull(prototype, "节点原型不能为空");

        // 将原型包装成 Supplier 再注册
        Supplier<DagNode<C, ?>> factory = () -> prototype;

        if (factoryMap.putIfAbsent(nodeTypeId, factory) != null) {
            throw new IllegalArgumentException(String.format("节点类型 ID '%s' 在上下文 '%s' 的注册表中已存在。",
                    nodeTypeId, contextType.getSimpleName()));
        }

        // 提取并缓存元数据
        NodeMetadata metadata = extractAndCacheMetadata(nodeTypeId, prototype);

        log.info("上下文 '{}': 已注册节点类型 '{}' (使用原型, 实现: {})",
                contextType.getSimpleName(), nodeTypeId, prototype.getClass().getName());
    }

    // 提取元数据并放入缓存
    private NodeMetadata extractAndCacheMetadata(String nodeTypeId, DagNode<C, ?> nodeInstance) {
        try {
            NodeMetadata metadata = extractMetadataInternal(nodeTypeId, nodeInstance);
            metadataMap.put(nodeTypeId, metadata);
            return metadata;
        } catch (Exception e) {
            // 如果提取元数据失败，也应该移除注册
            factoryMap.remove(nodeTypeId);
            log.error("提取节点类型 '{}' (实现: {}) 的元数据时出错。",
                    nodeTypeId, nodeInstance.getClass().getName(), e);
            throw new IllegalArgumentException("提取元数据失败: " + nodeTypeId, e);
        }
    }


    @Override
    public Optional<DagNode<C, ?>> getNodeInstance(String nodeTypeId) {
        Objects.requireNonNull(nodeTypeId, "节点类型 ID 不能为空");
        Supplier<? extends DagNode<C, ?>> factory = factoryMap.get(nodeTypeId);
        // 使用 Optional 处理 factory 可能为 null 的情况，并调用 get() 获取实例
        return Optional.ofNullable(factory).map(Supplier::get);
    }

    @Override
    public Optional<NodeMetadata> getNodeMetadata(String nodeTypeId) {
        Objects.requireNonNull(nodeTypeId, "节点类型 ID 不能为空");
        // 直接从缓存获取
        return Optional.ofNullable(metadataMap.get(nodeTypeId));
    }

    // 内部方法：实际提取元数据并进行基本校验
    private NodeMetadata extractMetadataInternal(String nodeTypeId, DagNode<C, ?> nodeInstance) {
        final Set<InputSlot<?>> inputSlots = nodeInstance.getInputSlots();
        final OutputSlot<?> primaryOutputSlot = nodeInstance.getOutputSlot();
        final Set<OutputSlot<?>> additionalOutputSlots = nodeInstance.getAdditionalOutputSlots();

        // 基本校验
        Objects.requireNonNull(inputSlots, "getInputSlots() 不能为节点类型 '" + nodeTypeId + "' 返回 null");
        Objects.requireNonNull(primaryOutputSlot, "getOutputSlot() 不能为节点类型 '" + nodeTypeId + "' 返回 null");
        Objects.requireNonNull(additionalOutputSlots, "getAdditionalOutputSlots() 不能为节点类型 '" + nodeTypeId + "' 返回 null");

        // 可以在这里添加更多验证，例如：
        // 1. 输入/输出槽 ID 在节点内部是否唯一？
        // 2. 默认输出槽 ID 是否正确使用？

        // 创建元数据实例 (使用匿名内部类或记录类)
        return new NodeMetadata() {
            // 返回不可变集合的副本，防止外部修改
            private final Set<InputSlot<?>> finalInputSlots = Collections.unmodifiableSet(new HashSet<>(inputSlots));
            private final Set<OutputSlot<?>> finalAdditionalOutputSlots = Collections.unmodifiableSet(new HashSet<>(additionalOutputSlots));

            @Override public String getTypeId() { return nodeTypeId; }
            @Override public Set<InputSlot<?>> getInputSlots() { return finalInputSlots; }
            @Override public OutputSlot<?> getPrimaryOutputSlot() { return primaryOutputSlot; }
            @Override public Set<OutputSlot<?>> getAdditionalOutputSlots() { return finalAdditionalOutputSlots; }

            @Override
            public String toString() {
                return String.format("Metadata[type=%s, inputs=%d, outputs=%d]",
                        nodeTypeId, finalInputSlots.size(), 1 + finalAdditionalOutputSlots.size());
            }
        };
    }
}
