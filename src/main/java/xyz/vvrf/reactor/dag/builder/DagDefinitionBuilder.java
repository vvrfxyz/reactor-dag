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
 * 新增了直接添加节点实现实例的方法。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen (Refactored & Modified)
 */
@Slf4j
public class DagDefinitionBuilder<C> {

    private final Class<C> contextType;
    private String dagName;
    private ErrorHandlingStrategy errorStrategy = ErrorHandlingStrategy.FAIL_FAST; // 默认策略

    // 注册的节点实现 (逻辑节点) - 仍然保留，用于 typeId 方式
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
     * 适用于需要通过类型 ID 多次实例化节点的场景。
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
     * 向图中添加一个节点实例，使用预先注册的实现类型 ID。
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
            throw new IllegalArgumentException(String.format("Node implementation type ID '%s' not registered for DAG '%s'. Cannot add instance '%s'.", typeId, dagName, instanceName));
        }

        // 使用 NodeBuildInfo 存储信息
        nodesInGraph.put(instanceName, new NodeBuildInfo<>(typeId, implementation));
        wiring.putIfAbsent(instanceName, new HashMap<>()); // 初始化连接 Map (如果不存在)
        log.debug("DAG '{}': Added node instance '{}' using registered implementation '{}'", dagName, instanceName, typeId);
        return this;
    }

    /**
     * 【新方法】向图中添加一个节点实例，直接提供节点实现。
     * 适用于不需要预先注册或复用类型 ID 的场景。
     *
     * @param instanceName   节点实例在图中的唯一名称
     * @param implementation 节点逻辑的实例 (不能为空)
     * @return this builder
     * @throws IllegalArgumentException 如果 instanceName 已存在
     */
    public DagDefinitionBuilder<C> addNode(String instanceName, DagNode<C, ?> implementation) {
        Objects.requireNonNull(instanceName, "Instance name cannot be null");
        Objects.requireNonNull(implementation, "Node implementation cannot be null for instance '" + instanceName + "'");

        if (nodesInGraph.containsKey(instanceName)) {
            throw new IllegalArgumentException(String.format("Node instance name '%s' already exists in DAG '%s'.", instanceName, dagName));
        }

        // 为直接添加的实现生成一个内部 typeId，主要用于日志和 NodeBuildInfo 结构一致性
        String internalTypeId = generateInternalTypeId(implementation);

        // 检查这个内部 typeId 是否与已注册的冲突 (概率极低，但可以加一层保护)
        if (registeredImplementations.containsKey(internalTypeId)) {
            log.warn("DAG '{}': Generated internal type ID '{}' for instance '{}' conflicts with a registered implementation ID. This might cause confusion.",
                    dagName, internalTypeId, instanceName);
            // 或者可以选择抛出异常，取决于严格程度
        }

        // 使用 NodeBuildInfo 存储信息
        nodesInGraph.put(instanceName, new NodeBuildInfo<>(internalTypeId, implementation));
        wiring.putIfAbsent(instanceName, new HashMap<>()); // 初始化连接 Map (如果不存在)
        log.debug("DAG '{}': Added node instance '{}' with direct implementation (Class: {}, Internal TypeId: {})",
                dagName, instanceName, implementation.getClass().getSimpleName(), internalTypeId);
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
            throw new IllegalArgumentException(String.format("Downstream node instance '%s' not found in DAG '%s'. Cannot wire input '%s'.", downstreamInstanceName, dagName, inputSlotName));
        }
        if (upstreamInfo == null) {
            throw new IllegalArgumentException(String.format("Upstream node instance '%s' not found in DAG '%s'. Cannot wire to input '%s' of '%s'.", upstreamInstanceName, dagName, inputSlotName, downstreamInstanceName));
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
            // 允许 Void 类型作为任何类型的输入源 (表示仅依赖执行完成，不关心数据)
            // 但下游必须显式声明需要 Void.class 或 Object.class
            // 这里维持严格类型检查，如果需要 Void 传递，上游应返回 NodeResult.success(context, Void.class)
            throw new IllegalArgumentException(String.format("Type mismatch for input '%s' of node '%s'. Required: %s, Provided by '%s' (Type: %s): %s",
                    inputSlotName, downstreamInstanceName, requiredInputType.getSimpleName(),
                    upstreamInstanceName, upstreamInfo.getTypeId(), upstreamOutputType.getSimpleName()));
        }

        // 检查输入槽是否已被连接
        Map<String, String> downstreamWiring = wiring.computeIfAbsent(downstreamInstanceName, k -> new HashMap<>()); // 确保 Map 存在
        if (downstreamWiring.containsKey(inputSlotName)) {
            throw new IllegalArgumentException(String.format("Input slot '%s' of node '%s' is already wired to '%s'. Cannot re-wire to '%s'.",
                    inputSlotName, downstreamInstanceName, downstreamWiring.get(inputSlotName), upstreamInstanceName));
        }

        downstreamWiring.put(inputSlotName, upstreamInstanceName);
        log.debug("DAG '{}': Wired output of '{}' (Type: {}) to input slot '{}' of '{}' (Type: {})",
                dagName, upstreamInstanceName, upstreamInfo.getTypeId(), inputSlotName, downstreamInstanceName, downstreamInfo.getTypeId());
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

        // 0. 预检查：确保所有添加的节点都有 NodeBuildInfo
        if (nodesInGraph.isEmpty() && !wiring.isEmpty()) {
            log.warn("DAG '{}': Wiring defined but no nodes were added.", dagName);
            // 可以选择清空 wiring 或继续，当前选择继续，让验证步骤处理
        }
        // (可以增加检查，确保 wiring 中的 key 和 value 都在 nodesInGraph 中，虽然 wire 方法已做检查)

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
            // 如果没有节点，返回空列表
            if (nodesInGraph.isEmpty()) {
                executionOrder = Collections.emptyList();
            } else {
                executionOrder = GraphUtils.topologicalSort(nodesInGraph.keySet(), wiring, dagName);
            }
        } catch (IllegalStateException e) {
            log.error("DAG '{}' topological sort failed during build: {}", dagName, e.getMessage());
            throw e;
        }

        log.info("DAG '{}' build successful. {} nodes found. Execution order: {}", dagName, nodesInGraph.size(), executionOrder);
        printDagStructure(); // 打印结构信息

        // 4. 创建不可变的 DagDefinition 实例
        return new DefaultDagDefinition<>(
                dagName,
                contextType,
                errorStrategy,
                Collections.unmodifiableMap(new HashMap<>(nodesInGraph)), // 创建不可变副本
                deepUnmodifiableMap(wiring), // 确保内部 Map 也不可变
                executionOrder // topologicalSort 返回的是不可变列表
        );
    }

    // --- Helper Methods ---

    // 为直接添加的实现生成内部 Type ID
    private String generateInternalTypeId(DagNode<C, ?> implementation) {
        // 使用类名 + @ + identityHashCode 保证唯一性，同时有一定可读性
        return implementation.getClass().getName() + "@" + System.identityHashCode(implementation);
    }

    private void printDagStructure() {
        if (!log.isInfoEnabled() || nodesInGraph.isEmpty()) {
            return;
        }

        log.info("DAG '{}' Final Structure:", dagName);
        StringBuilder builder = new StringBuilder("\n");
        // 调整列宽以适应可能更长的 Type ID
        builder.append(String.format("%-30s | %-50s | %-40s | %-40s\n",
                "Instance Name", "Implementation Type / ID", "Inputs (Slot <- Upstream)", "Outputs To (Downstream -> Slot)"));
        StringBuilder divider = new StringBuilder();
        for (int i = 0; i < 175; i++) { // 增加分隔线长度
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

        // 按实例名称排序打印
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

            // 打印 Type ID
            builder.append(String.format("%-30s | %-50s | %-40s | %-40s\n",
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
        private final String typeId; // 可以是注册的 ID 或内部生成的 ID
        private final DagNode<C, ?> implementation;
        private final Map<String, Class<?>> inputRequirements;
        private final Class<?> outputType;

        NodeBuildInfo(String typeId, DagNode<C, ?> implementation) {
            this.typeId = Objects.requireNonNull(typeId, "NodeBuildInfo typeId cannot be null");
            this.implementation = Objects.requireNonNull(implementation, "NodeBuildInfo implementation cannot be null");
            // 从实现中获取输入输出信息
            this.inputRequirements = implementation.getInputRequirements() != null
                    ? Collections.unmodifiableMap(new HashMap<>(implementation.getInputRequirements())) // 确保不可变
                    : Collections.emptyMap();
            this.outputType = implementation.getPayloadType();
            if (this.outputType == null) {
                // 增加校验：节点实现必须提供有效的 Payload 类型
                throw new IllegalArgumentException("DagNode implementation " + implementation.getClass().getName() +
                        " (used for typeId '" + typeId + "') must return a non-null payload type from getPayloadType().");
            }
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
            this.executionOrder = executionOrder; // 已是不可变 List
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
