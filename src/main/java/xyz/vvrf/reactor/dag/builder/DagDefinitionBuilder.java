// file: builder/DagDefinitionBuilder.java
package xyz.vvrf.reactor.dag.builder;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.ErrorHandlingStrategy;
import xyz.vvrf.reactor.dag.util.GraphUtils; // 引入图工具类

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 用于以编程方式构建 DagDefinition 的构建器。
 * 支持注册节点实现、添加节点实例和定义它们之间的连接。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen (Refactored)
 */
@Slf4j
public class DagDefinitionBuilder<C> {

    private final Class<C> contextType;
    private String dagName;
    private ErrorHandlingStrategy errorStrategy = ErrorHandlingStrategy.FAIL_FAST; // 默认策略

    // 注册的节点实现 (逻辑节点)
    private final Map<String, DagNode<C, ?>> registeredImplementations = new ConcurrentHashMap<>();

    // 图中定义的节点实例
    // Key: instanceName, Value: NodeBuildInfo (包含 typeId 和输入需求等)
    private final Map<String, NodeBuildInfo<C>> nodesInGraph = new ConcurrentHashMap<>();

    // 图中定义的连接关系
    // Key: downstreamInstanceName, Value: Map<inputSlotName, upstreamInstanceName>
    private final Map<String, Map<String, String>> wiring = new ConcurrentHashMap<>();

    /**
     * 创建 DAG 定义构建器。
     *
     * @param contextType DAG 适用的上下文类型
     * @param dagName     DAG 的名称
     */
    public DagDefinitionBuilder(Class<C> contextType, String dagName) {
        this.contextType = Objects.requireNonNull(contextType, "Context type cannot be null");
        this.dagName = Objects.requireNonNull(dagName, "DAG name cannot be null");
        log.info("Creating DagDefinitionBuilder for context '{}', DAG name '{}'", contextType.getSimpleName(), dagName);
    }

    /**
     * 设置 DAG 的名称。
     */
    public DagDefinitionBuilder<C> name(String name) {
        this.dagName = Objects.requireNonNull(name, "DAG name cannot be null");
        return this;
    }

    /**
     * 设置错误处理策略。
     */
    public DagDefinitionBuilder<C> errorStrategy(ErrorHandlingStrategy strategy) {
        this.errorStrategy = Objects.requireNonNull(strategy, "Error handling strategy cannot be null");
        return this;
    }

    /**
     * 注册一个可复用的节点实现 (逻辑节点)。
     *
     * @param typeId             此节点实现的唯一标识符 (例如，类的全名或自定义 ID)
     * @param nodeImplementation 节点逻辑的实例
     * @return this builder
     * @throws IllegalArgumentException 如果 typeId 已被注册
     */
    public DagDefinitionBuilder<C> registerImplementation(String typeId, DagNode<C, ?> nodeImplementation) {
        Objects.requireNonNull(typeId, "Type ID cannot be null");
        Objects.requireNonNull(nodeImplementation, "Node implementation cannot be null");
        if (registeredImplementations.containsKey(typeId)) {
            throw new IllegalArgumentException(String.format("Node implementation type ID '%s' is already registered.", typeId));
        }
        registeredImplementations.put(typeId, nodeImplementation);
        log.debug("DAG '{}': Registered node implementation '{}' (Class: {})",
                dagName, typeId, nodeImplementation.getClass().getSimpleName());
        return this;
    }

    /**
     * 向图中添加一个节点实例。
     *
     * @param instanceName 节点实例在图中的唯一名称
     * @param typeId       要使用的已注册节点实现的 ID
     * @return this builder
     * @throws IllegalArgumentException 如果 instanceName 已存在或 typeId 未注册
     */
    public DagDefinitionBuilder<C> addNode(String instanceName, String typeId) {
        Objects.requireNonNull(instanceName, "Instance name cannot be null");
        Objects.requireNonNull(typeId, "Type ID cannot be null");

        if (nodesInGraph.containsKey(instanceName)) {
            throw new IllegalArgumentException(String.format("Node instance name '%s' already exists in DAG '%s'.", instanceName, dagName));
        }
        DagNode<C, ?> implementation = registeredImplementations.get(typeId);
        if (implementation == null) {
            throw new IllegalArgumentException(String.format("Node implementation type ID '%s' not registered for DAG '%s'.", typeId, dagName));
        }

        nodesInGraph.put(instanceName, new NodeBuildInfo<>(typeId, implementation));
        wiring.put(instanceName, new HashMap<>()); // 初始化连接 Map
        log.debug("DAG '{}': Added node instance '{}' using implementation '{}'", dagName, instanceName, typeId);
        return this;
    }

    /**
     * 定义一个连接：将上游节点的输出连接到下游节点的指定输入槽。
     *
     * @param downstreamInstanceName 接收输入的下游节点实例名称
     * @param inputSlotName          下游节点声明的输入槽逻辑名称 (必须在其 getInputRequirements 中)
     * @param upstreamInstanceName   提供输入的上游节点实例名称
     * @return this builder
     * @throws IllegalArgumentException 如果节点实例不存在、输入槽无效、输入槽已被连接，或类型不匹配
     */
    public DagDefinitionBuilder<C> wire(String downstreamInstanceName, String inputSlotName, String upstreamInstanceName) {
        Objects.requireNonNull(downstreamInstanceName, "Downstream instance name cannot be null");
        Objects.requireNonNull(inputSlotName, "Input slot name cannot be null");
        Objects.requireNonNull(upstreamInstanceName, "Upstream instance name cannot be null");

        NodeBuildInfo<C> downstreamInfo = nodesInGraph.get(downstreamInstanceName);
        NodeBuildInfo<C> upstreamInfo = nodesInGraph.get(upstreamInstanceName);

        if (downstreamInfo == null) {
            throw new IllegalArgumentException(String.format("Downstream node instance '%s' not found in DAG '%s'.", downstreamInstanceName, dagName));
        }
        if (upstreamInfo == null) {
            throw new IllegalArgumentException(String.format("Upstream node instance '%s' not found in DAG '%s'.", upstreamInstanceName, dagName));
        }

        // 检查下游节点是否声明了此输入槽
        Class<?> requiredInputType = downstreamInfo.getInputRequirements().get(inputSlotName);
        if (requiredInputType == null) {
            throw new IllegalArgumentException(String.format("Downstream node '%s' (Type: %s) does not declare input slot '%s'. Declared slots: %s",
                    downstreamInstanceName, downstreamInfo.getTypeId(), inputSlotName, downstreamInfo.getInputRequirements().keySet()));
        }

        // 检查上游节点的输出类型是否兼容
        Class<?> upstreamOutputType = upstreamInfo.getOutputType();
        if (!requiredInputType.isAssignableFrom(upstreamOutputType)) {
            throw new IllegalArgumentException(String.format("Type mismatch for input '%s' of node '%s'. Required: %s, Provided by '%s': %s",
                    inputSlotName, downstreamInstanceName, requiredInputType.getSimpleName(),
                    upstreamInstanceName, upstreamOutputType.getSimpleName()));
        }

        // 检查输入槽是否已被连接
        Map<String, String> downstreamWiring = wiring.get(downstreamInstanceName);
        if (downstreamWiring.containsKey(inputSlotName)) {
            throw new IllegalArgumentException(String.format("Input slot '%s' of node '%s' is already wired to '%s'.",
                    inputSlotName, downstreamInstanceName, downstreamWiring.get(inputSlotName)));
        }

        downstreamWiring.put(inputSlotName, upstreamInstanceName);
        log.debug("DAG '{}': Wired output of '{}' to input slot '{}' of '{}'",
                dagName, upstreamInstanceName, inputSlotName, downstreamInstanceName);
        return this;
    }

    /**
     * 构建最终的、不可变的 DagDefinition。
     * 此方法会执行图的验证和拓扑排序。
     *
     * @return DagDefinition 实例
     * @throws IllegalStateException 如果图验证失败 (例如，存在循环、类型不匹配、节点未连接但有输入需求等)
     */
    public DagDefinition<C> build() {
        log.info("Building DagDefinition for '{}'...", dagName);

        // 1. 验证图结构 (使用 GraphUtils)
        try {
            GraphUtils.validateGraphStructure(nodesInGraph, wiring, dagName);
        } catch (IllegalStateException e) {
            log.error("DAG '{}' validation failed during build: {}", dagName, e.getMessage());
            throw e;
        }

        // 2. 检测循环 (使用 GraphUtils)
        try {
            GraphUtils.detectCycles(nodesInGraph.keySet(), wiring, dagName);
        } catch (IllegalStateException e) {
            log.error("DAG '{}' cycle detection failed during build: {}", dagName, e.getMessage());
            throw e;
        }

        // 3. 计算拓扑排序 (使用 GraphUtils)
        List<String> executionOrder;
        try {
            executionOrder = GraphUtils.topologicalSort(nodesInGraph.keySet(), wiring, dagName);
        } catch (IllegalStateException e) {
            log.error("DAG '{}' topological sort failed during build: {}", dagName, e.getMessage());
            throw e;
        }

        log.info("DAG '{}' build successful. Execution order: {}", dagName, executionOrder);
        printDagStructure(); // 打印结构信息

        // 4. 创建不可变的 DagDefinition 实例
        return new DefaultDagDefinition<>(
                dagName,
                contextType,
                errorStrategy,
                Collections.unmodifiableMap(nodesInGraph),
                deepUnmodifiableMap(wiring), // 确保内部 Map 也不可变
                executionOrder
        );
    }

    // --- Helper Methods ---

    private void printDagStructure() {
        if (!log.isInfoEnabled() || nodesInGraph.isEmpty()) {
            return;
        }

        log.info("DAG '{}' Final Structure:", dagName);
        StringBuilder builder = new StringBuilder("\n");
        builder.append(String.format("%-30s | %-30s | %-40s | %-40s\n",
                "Instance Name", "Implementation Type", "Inputs (Slot <- Upstream)", "Outputs To (Downstream -> Slot)"));
        StringBuilder divider = new StringBuilder();
        for (int i = 0; i < 155; i++) {
            divider.append("-");
        }
        builder.append(divider).append("\n");

        // 计算每个节点的下游连接
        Map<String, List<String>> downstreamConnections = new HashMap<>();
        wiring.forEach((downstream, connections) -> {
            connections.forEach((slot, upstream) -> {
                downstreamConnections.computeIfAbsent(upstream, k -> new ArrayList<>())
                        .add(String.format("%s -> %s", downstream, slot));
            });
        });

        List<String> nodeNames = nodesInGraph.keySet().stream().sorted().collect(Collectors.toList());

        for (String instanceName : nodeNames) {
            NodeBuildInfo<C> info = nodesInGraph.get(instanceName);
            Map<String, String> inputs = wiring.getOrDefault(instanceName, Collections.emptyMap());
            List<String> outputs = downstreamConnections.getOrDefault(instanceName, Collections.emptyList());

            String inputsStr = inputs.entrySet().stream()
                    .map(e -> String.format("%s <- %s", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(", "));
            if (inputsStr.isEmpty()) {
                inputsStr = "None";
            }

            String outputsStr = String.join(", ", outputs);
            if (outputsStr.isEmpty()) {
                outputsStr = "None";
            }

            builder.append(String.format("%-30s | %-30s | %-40s | %-40s\n",
                    instanceName, info.getTypeId(), inputsStr, outputsStr));
        }
        log.info(builder.toString());
    }


    // 创建深度不可变的 Map<String, Map<String, String>>
    private static Map<String, Map<String, String>> deepUnmodifiableMap(Map<String, Map<String, String>> original) {
        Map<String, Map<String, String>> mutableCopy = new HashMap<>();
        original.forEach((key, value) -> mutableCopy.put(key, Collections.unmodifiableMap(new HashMap<>(value))));
        return Collections.unmodifiableMap(mutableCopy);
    }

    // 内部类，用于存储构建时的节点信息
    @Getter
    public static class NodeBuildInfo<C> {
        private final String typeId;
        private final DagNode<C, ?> implementation;
        private final Map<String, Class<?>> inputRequirements;
        private final Class<?> outputType;

        NodeBuildInfo(String typeId, DagNode<C, ?> implementation) {
            this.typeId = typeId;
            this.implementation = implementation;
            this.inputRequirements = implementation.getInputRequirements(); // 获取输入需求
            this.outputType = implementation.getPayloadType();      // 获取输出类型
        }
    }

    // 内部类，表示最终的不可变 DAG 定义
    private static class DefaultDagDefinition<C> implements DagDefinition<C> {
        private final String dagName;
        private final Class<C> contextType;
        private final ErrorHandlingStrategy errorStrategy;
        private final Map<String, NodeBuildInfo<C>> nodeInfoMap; // 使用构建信息
        private final Map<String, Map<String, String>> wiring;
        private final List<String> executionOrder;
        private final Set<String> allNodeNames; // 缓存节点名称集合

        DefaultDagDefinition(String dagName, Class<C> contextType, ErrorHandlingStrategy errorStrategy,
                             Map<String, NodeBuildInfo<C>> nodeInfoMap,
                             Map<String, Map<String, String>> wiring,
                             List<String> executionOrder) {
            this.dagName = dagName;
            this.contextType = contextType;
            this.errorStrategy = errorStrategy;
            this.nodeInfoMap = nodeInfoMap; // 已是不可变 Map
            this.wiring = wiring;           // 已是深度不可变 Map
            this.executionOrder = Collections.unmodifiableList(new ArrayList<>(executionOrder)); // 确保列表不可变
            this.allNodeNames = Collections.unmodifiableSet(nodeInfoMap.keySet());
        }

        @Override
        public String getDagName() {
            return dagName;
        }

        @Override
        public Class<C> getContextType() {
            return contextType;
        }

        @Override
        public List<String> getExecutionOrder() {
            return executionOrder;
        }

        @Override
        public Optional<DagNode<C, ?>> getNodeImplementation(String instanceName) {
            return Optional.ofNullable(nodeInfoMap.get(instanceName))
                    .map(NodeBuildInfo::getImplementation);
        }

        @Override
        public Optional<Class<?>> getNodeOutputType(String instanceName) {
            return Optional.ofNullable(nodeInfoMap.get(instanceName))
                    .map(NodeBuildInfo::getOutputType);
        }

        @Override
        public Map<String, String> getUpstreamWiring(String instanceName) {
            // wiring 本身是不可变的，其内部 Map 也是不可变的
            return wiring.getOrDefault(instanceName, Collections.emptyMap());
        }

        @Override
        public ErrorHandlingStrategy getErrorHandlingStrategy() {
            return errorStrategy;
        }

        @Override
        public Set<String> getAllNodeInstanceNames() {
            return allNodeNames;
        }
    }
}
