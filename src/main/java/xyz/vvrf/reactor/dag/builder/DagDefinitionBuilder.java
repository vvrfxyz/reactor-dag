// file: builder/DagDefinitionBuilder.java
package xyz.vvrf.reactor.dag.builder;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;
import xyz.vvrf.reactor.dag.util.GraphUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 用于以编程方式构建 DagDefinition 数据结构的构建器。
 * 需要一个 NodeRegistry 来验证节点类型和插槽。
 *
 * @param <C> 上下文类型
 * @author Refactored
 */
@Slf4j
public class DagDefinitionBuilder<C> {

    private final Class<C> contextType;
    private final NodeRegistry<C> nodeRegistry; // 需要注册表进行验证
    private String dagName;
    private ErrorHandlingStrategy errorStrategy = ErrorHandlingStrategy.FAIL_FAST;

    // 存储节点定义
    private final Map<String, NodeDefinition> nodeDefinitions = new LinkedHashMap<>(); // 使用 LinkedHashMap 保持添加顺序
    // 存储边定义
    private final List<EdgeDefinition<C>> edgeDefinitions = new ArrayList<>();

    /**
     * 创建 DAG 定义构建器。
     *
     * @param contextType  DAG 适用的上下文类型
     * @param dagName      DAG 的名称
     * @param nodeRegistry 用于验证节点类型和获取元数据的注册表
     */
    public DagDefinitionBuilder(Class<C> contextType, String dagName, NodeRegistry<C> nodeRegistry) {
        this.contextType = Objects.requireNonNull(contextType, "Context type cannot be null");
        this.dagName = Objects.requireNonNull(dagName, "DAG name cannot be null");
        this.nodeRegistry = Objects.requireNonNull(nodeRegistry, "NodeRegistry cannot be null");
        log.info("Creating DagDefinitionBuilder for context '{}', DAG name '{}'", contextType.getSimpleName(), dagName);
    }

    public DagDefinitionBuilder<C> name(String name) {
        this.dagName = Objects.requireNonNull(name, "DAG name cannot be null");
        return this;
    }

    public DagDefinitionBuilder<C> errorStrategy(ErrorHandlingStrategy strategy) {
        this.errorStrategy = Objects.requireNonNull(strategy, "Error handling strategy cannot be null");
        return this;
    }

    /**
     * 向图中添加一个节点定义。
     *
     * @param instanceName    节点实例在图中的唯一名称
     * @param nodeTypeId      要使用的节点类型 ID (必须在 NodeRegistry 中注册)
     * @param configuration   可选的节点特定配置
     * @return this builder
     * @throws IllegalArgumentException 如果 instanceName 已存在或 nodeTypeId 未在注册表中找到
     */
    public DagDefinitionBuilder<C> addNode(String instanceName, String nodeTypeId, Map<String, Object> configuration) {
        Objects.requireNonNull(instanceName, "Instance name cannot be null");
        Objects.requireNonNull(nodeTypeId, "Node type ID cannot be null");

        if (nodeDefinitions.containsKey(instanceName)) {
            throw new IllegalArgumentException(String.format("Node instance name '%s' already exists in DAG '%s'.", instanceName, dagName));
        }
        // 验证 nodeTypeId 是否存在于注册表
//        if (nodeRegistry.getNodeMetadata(nodeTypeId).isEmpty()) {
//            throw new IllegalArgumentException(String.format("Node type ID '%s' not found in NodeRegistry. Cannot add instance '%s' for DAG '%s'.", nodeTypeId, instanceName, dagName));
//        }

        NodeDefinition nodeDef = new NodeDefinition(instanceName, nodeTypeId, configuration);
        nodeDefinitions.put(instanceName, nodeDef);
        log.debug("DAG '{}': Added node definition '{}' (Type: {})", dagName, instanceName, nodeTypeId);
        return this;
    }

    public DagDefinitionBuilder<C> addNode(String instanceName, String nodeTypeId) {
        return addNode(instanceName, nodeTypeId, null);
    }

    /**
     * 定义一个连接边。
     *
     * @param upstreamInstanceName   提供输入的上游节点实例名称
     * @param outputSlotId           上游节点声明的输出槽 ID
     * @param downstreamInstanceName 接收输入的下游节点实例名称
     * @param inputSlotId            下游节点声明的输入槽 ID
     * @param condition              边的激活条件 (可选, null 表示无条件)
     * @return this builder
     * @throws IllegalArgumentException 如果节点实例不存在、插槽 ID 无效、或插槽类型不匹配
     */
    public DagDefinitionBuilder<C> addEdge(String upstreamInstanceName, String outputSlotId,
                                           String downstreamInstanceName, String inputSlotId,
                                           Condition<C> condition) {
        Objects.requireNonNull(upstreamInstanceName, "Upstream instance name cannot be null");
        Objects.requireNonNull(outputSlotId, "Output slot ID cannot be null");
        Objects.requireNonNull(downstreamInstanceName, "Downstream instance name cannot be null");
        Objects.requireNonNull(inputSlotId, "Input slot ID cannot be null");

        NodeDefinition upstreamDef = nodeDefinitions.get(upstreamInstanceName);
        NodeDefinition downstreamDef = nodeDefinitions.get(downstreamInstanceName);

        if (upstreamDef == null) {
            throw new IllegalArgumentException(String.format("Upstream node instance '%s' not found in DAG '%s'. Cannot add edge.", upstreamInstanceName, dagName));
        }
        if (downstreamDef == null) {
            throw new IllegalArgumentException(String.format("Downstream node instance '%s' not found in DAG '%s'. Cannot add edge.", downstreamInstanceName, dagName));
        }

        // 从注册表获取元数据进行验证
        NodeRegistry.NodeMetadata upstreamMeta = nodeRegistry.getNodeMetadata(upstreamDef.getNodeTypeId())
                .orElseThrow(() -> new IllegalStateException("Upstream node type '" + upstreamDef.getNodeTypeId() + "' metadata not found in registry."));
        NodeRegistry.NodeMetadata downstreamMeta = nodeRegistry.getNodeMetadata(downstreamDef.getNodeTypeId())
                .orElseThrow(() -> new IllegalStateException("Downstream node type '" + downstreamDef.getNodeTypeId() + "' metadata not found in registry."));

        // 验证插槽存在性和类型兼容性
        OutputSlot<?> outputSlot = upstreamMeta.findOutputSlot(outputSlotId)
                .orElseThrow(() -> new IllegalArgumentException(String.format("Output slot '%s' not found for node type '%s' (Instance: '%s').",
                        outputSlotId, upstreamDef.getNodeTypeId(), upstreamInstanceName)));

        InputSlot<?> inputSlot = downstreamMeta.findInputSlot(inputSlotId)
                .orElseThrow(() -> new IllegalArgumentException(String.format("Input slot '%s' not found for node type '%s' (Instance: '%s').",
                        inputSlotId, downstreamDef.getNodeTypeId(), downstreamInstanceName)));

        // 核心类型检查！
        if (!inputSlot.getType().isAssignableFrom(outputSlot.getType())) {
            throw new IllegalArgumentException(String.format("Type mismatch for edge %s[%s] -> %s[%s]. Required input type: %s, Provided output type: %s.",
                    upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId,
                    inputSlot.getType().getName(), outputSlot.getType().getName()));
        }

        // 检查下游输入槽是否已被相同上游的相同输出槽连接（允许来自不同上游连接到同一下游输入槽，但不允许完全重复的边）
        boolean edgeExists = edgeDefinitions.stream().anyMatch(e ->
                e.getDownstreamInstanceName().equals(downstreamInstanceName) &&
                        e.getInputSlotId().equals(inputSlotId) &&
                        e.getUpstreamInstanceName().equals(upstreamInstanceName) &&
                        e.getOutputSlotId().equals(outputSlotId));
        if (edgeExists) {
            throw new IllegalArgumentException(String.format("Duplicate edge detected: %s[%s] -> %s[%s]",
                    upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId));
        }
        // 注意：允许多个不同的上游连接到同一个下游输入槽。执行引擎需要处理这种情况（例如，取第一个到达的？合并？具体策略待定，目前 InputAccessor 只会暴露一个值）。

        EdgeDefinition<C> edgeDef = new EdgeDefinition<>(upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, condition);
        edgeDefinitions.add(edgeDef);
        log.debug("DAG '{}': Added edge {}[{}] -> {}[{}] {}", dagName, upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, condition != null ? "with condition" : "unconditionally");
        return this;
    }

    // 重载方法，用于无条件边
    public DagDefinitionBuilder<C> addEdge(String upstreamInstanceName, String outputSlotId,
                                           String downstreamInstanceName, String inputSlotId) {
        return addEdge(upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, null);
    }

    // 简化方法，使用默认输出槽 ID
    public DagDefinitionBuilder<C> addEdge(String upstreamInstanceName,
                                           String downstreamInstanceName, String inputSlotId,
                                           Condition<C> condition) {
        return addEdge(upstreamInstanceName, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, downstreamInstanceName, inputSlotId, condition);
    }
    public DagDefinitionBuilder<C> addEdge(String upstreamInstanceName,
                                           String downstreamInstanceName, String inputSlotId) {
        return addEdge(upstreamInstanceName, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, downstreamInstanceName, inputSlotId, null);
    }


    /**
     * 构建最终的、不可变的 DagDefinition 数据结构。
     * 此方法会执行图的验证和拓扑排序。
     *
     * @return DagDefinition 实例
     * @throws IllegalStateException 如果图验证失败
     */
    public DagDefinition<C> build() {
        log.info("Building DagDefinition for '{}'...", dagName);

        // 验证图结构 (需要传入 NodeRegistry 以获取元数据)
        try {
            GraphUtils.validateGraphStructure(nodeDefinitions, edgeDefinitions, nodeRegistry, dagName);
        } catch (IllegalStateException e) {
            log.error("DAG '{}' validation failed during build: {}", dagName, e.getMessage());
            throw e;
        }

        // 检测循环
        try {
            GraphUtils.detectCycles(nodeDefinitions.keySet(), edgeDefinitions, dagName);
        } catch (IllegalStateException e) {
            log.error("DAG '{}' cycle detection failed during build: {}", dagName, e.getMessage());
            throw e;
        }

        // 计算拓扑排序
        List<String> executionOrder;
        try {
            if (nodeDefinitions.isEmpty()) {
                executionOrder = Collections.emptyList();
            } else {
                executionOrder = GraphUtils.topologicalSort(nodeDefinitions.keySet(), edgeDefinitions, dagName);
            }
        } catch (IllegalStateException e) {
            log.error("DAG '{}' topological sort failed during build: {}", dagName, e.getMessage());
            throw e;
        }

        log.info("DAG '{}' build successful. {} nodes, {} edges. Execution order: {}", dagName, nodeDefinitions.size(), edgeDefinitions.size(), executionOrder);
        printDagStructure(); // 打印结构信息

        // 创建不可变的 DagDefinition 实例
        return new DefaultDagDefinition<>(
                dagName,
                contextType,
                errorStrategy,
                Collections.unmodifiableMap(new HashMap<>(nodeDefinitions)), // 复制一份确保不可变
                Collections.unmodifiableList(new ArrayList<>(edgeDefinitions)), // 复制一份确保不可变
                executionOrder // topologicalSort 返回的是不可变列表
        );
    }

    // --- Helper Methods ---

    private void printDagStructure() {
        if (!log.isInfoEnabled() || nodeDefinitions.isEmpty()) {
            return;
        }
        // ... (打印逻辑需要更新以反映新的 EdgeDefinition 结构)
        log.info("DAG '{}' Final Structure:", dagName);
        StringBuilder builder = new StringBuilder("\nNodes:\n");
        builder.append(String.format("%-30s | %-40s | %s\n", "Instance Name", "Type ID", "Configuration"));
        builder
//                .append("-".repeat(100))
                .append("\n");
        nodeDefinitions.forEach((name, def) -> builder.append(String.format("%-30s | %-40s | %s\n", name, def.getNodeTypeId(), def.getConfiguration())));

        builder.append("\nEdges:\n");
        builder.append(String.format("%-30s [%-20s] ---> %-30s [%-20s] | %s\n", "Upstream", "Output Slot", "Downstream", "Input Slot", "Condition"));
        builder
//                .append("-".repeat(120))
                .append("\n");
        edgeDefinitions.forEach(edge -> builder.append(String.format("%-30s [%-20s] ---> %-30s [%-20s] | %s\n",
                edge.getUpstreamInstanceName(), edge.getOutputSlotId(),
                edge.getDownstreamInstanceName(), edge.getInputSlotId(),
                edge.getCondition() == Condition.alwaysTrue() ? "Always True" : edge.getCondition().getClass().getSimpleName())));

        log.info(builder.toString());
    }

    // 内部类，表示最终的不可变 DAG 定义实现
    private static class DefaultDagDefinition<C> implements DagDefinition<C> {
        private final String dagName;
        private final Class<C> contextType;
        private final ErrorHandlingStrategy errorStrategy;
        private final Map<String, NodeDefinition> nodeDefinitions; // InstanceName -> NodeDefinition
        private final List<EdgeDefinition<C>> edgeDefinitions;
        private final List<String> executionOrder;
        private final Set<String> allNodeNames;
        // 预处理边，方便查找
        private final Map<String, List<EdgeDefinition<C>>> incomingEdgesMap;
        private final Map<String, List<EdgeDefinition<C>>> outgoingEdgesMap;


        DefaultDagDefinition(String dagName, Class<C> contextType, ErrorHandlingStrategy errorStrategy,
                             Map<String, NodeDefinition> nodeDefinitions,
                             List<EdgeDefinition<C>> edgeDefinitions,
                             List<String> executionOrder) {
            this.dagName = dagName;
            this.contextType = contextType;
            this.errorStrategy = errorStrategy;
            this.nodeDefinitions = nodeDefinitions; // 已是不可变 Map
            this.edgeDefinitions = edgeDefinitions; // 已是不可变 List
            this.executionOrder = executionOrder;   // 已是不可变 List
            this.allNodeNames = Collections.unmodifiableSet(nodeDefinitions.keySet());

            // 预处理边
            this.incomingEdgesMap = edgeDefinitions.stream()
                    .collect(Collectors.groupingBy(EdgeDefinition::getDownstreamInstanceName,
                            Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList)));
            this.outgoingEdgesMap = edgeDefinitions.stream()
                    .collect(Collectors.groupingBy(EdgeDefinition::getUpstreamInstanceName,
                            Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList)));
        }

        @Override public String getDagName() { return dagName; }
        @Override public Class<C> getContextType() { return contextType; }
        @Override public List<String> getExecutionOrder() { return executionOrder; }
        @Override public Map<String, NodeDefinition> getNodeDefinitions() { return nodeDefinitions; }
        @Override public Optional<NodeDefinition> getNodeDefinition(String instanceName) { return Optional.ofNullable(nodeDefinitions.get(instanceName)); }
        @Override public List<EdgeDefinition<C>> getEdgeDefinitions() { return edgeDefinitions; }
        @Override public List<EdgeDefinition<C>> getIncomingEdges(String downstreamInstanceName) { return incomingEdgesMap.getOrDefault(downstreamInstanceName, Collections.emptyList()); }
        @Override public List<EdgeDefinition<C>> getOutgoingEdges(String upstreamInstanceName) { return outgoingEdgesMap.getOrDefault(upstreamInstanceName, Collections.emptyList()); }
        @Override public ErrorHandlingStrategy getErrorHandlingStrategy() { return errorStrategy; }
        @Override public Set<String> getAllNodeInstanceNames() { return allNodeNames; }
    }
}
