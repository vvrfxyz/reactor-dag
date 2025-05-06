// 文件名: builder/DagDefinitionBuilder.java (已修改)
package xyz.vvrf.reactor.dag.builder;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.util.retry.Retry; // 引入 Retry
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.execution.StandardNodeExecutor; // 引入用于配置键
import xyz.vvrf.reactor.dag.registry.NodeRegistry;
import xyz.vvrf.reactor.dag.util.GraphUtils;

import java.time.Duration; // 引入 Duration
import java.util.*;
import java.util.stream.Collectors;

/**
 * 用于以编程方式构建不可变的 DagDefinition 数据结构。
 * 需要一个 NodeRegistry 来验证节点类型和插槽。
 * 允许实例特定的配置（重试、超时）。
 * 新增：构建成功后可生成 DOT 图形描述代码。
 *
 * @param <C> 上下文类型。
 * @author Refactored
 */
@Slf4j
public class DagDefinitionBuilder<C> {

    private final Class<C> contextType;
    private final NodeRegistry<C> nodeRegistry; // 仍然需要注册表进行验证
    private String dagName;
    private ErrorHandlingStrategy errorStrategy = ErrorHandlingStrategy.FAIL_FAST;

    // 存储节点定义（包括实例配置）
    private final Map<String, NodeDefinition> nodeDefinitions = new LinkedHashMap<>(); // 保持插入顺序
    // 存储边定义
    private final List<EdgeDefinition<C>> edgeDefinitions = new ArrayList<>();

    /**
     * 创建 DAG 定义构建器。
     *
     * @param contextType  DAG 操作的上下文类型。
     * @param dagName      DAG 的名称。
     * @param nodeRegistry 用于验证节点类型和获取元数据的注册表。
     */
    public DagDefinitionBuilder(Class<C> contextType, String dagName, NodeRegistry<C> nodeRegistry) {
        this.contextType = Objects.requireNonNull(contextType, "上下文类型不能为空");
        this.dagName = Objects.requireNonNull(dagName, "DAG 名称不能为空");
        this.nodeRegistry = Objects.requireNonNull(nodeRegistry, "NodeRegistry 不能为空");
        log.info("为上下文 '{}', DAG 名称 '{}' 创建 DagDefinitionBuilder", contextType.getSimpleName(), dagName);
    }

    public DagDefinitionBuilder<C> name(String name) {
        this.dagName = Objects.requireNonNull(name, "DAG 名称不能为空");
        return this;
    }

    public DagDefinitionBuilder<C> errorStrategy(ErrorHandlingStrategy strategy) {
        this.errorStrategy = Objects.requireNonNull(strategy, "错误处理策略不能为空");
        return this;
    }

    /**
     * 向图中添加一个节点实例定义。
     *
     * @param instanceName 节点实例在 DAG 中的唯一名称。
     * @param nodeTypeId   要使用的节点逻辑的类型 ID（必须在 NodeRegistry 中注册）。
     * @return this builder。
     * @throws IllegalArgumentException 如果 instanceName 已存在或 nodeTypeId 在注册表中未找到。
     */
    public DagDefinitionBuilder<C> addNode(String instanceName, String nodeTypeId) {
        Objects.requireNonNull(instanceName, "实例名称不能为空");
        Objects.requireNonNull(nodeTypeId, "节点类型 ID 不能为空");

        if (nodeDefinitions.containsKey(instanceName)) {
            throw new IllegalArgumentException(String.format("节点实例名称 '%s' 在 DAG '%s' 中已存在。", instanceName, dagName));
        }
        // 验证 nodeTypeId 是否存在于注册表（通过获取元数据隐式验证）
        if (!nodeRegistry.getNodeMetadata(nodeTypeId).isPresent()) {
            throw new IllegalArgumentException(String.format("在 NodeRegistry (上下文: '%s') 中找不到节点类型 ID '%s'。无法为 DAG '%s' 添加实例 '%s'。",
                    nodeTypeId, contextType.getSimpleName(), instanceName, dagName));
        }

        // 创建 NodeDefinition，初始配置为空 Map
        NodeDefinition nodeDef = new NodeDefinition(instanceName, nodeTypeId, new HashMap<>()); // 内部暂时使用可变 Map
        nodeDefinitions.put(instanceName, nodeDef);
        log.debug("DAG '{}': 添加了节点定义 '{}' (类型: {})", dagName, instanceName, nodeTypeId);
        return this;
    }

    // --- 实例配置方法 ---

    /**
     * 为特定节点实例设置自定义重试策略。
     * 这将覆盖节点实现中定义的默认重试策略。
     *
     * @param instanceName 要配置的节点实例的名称。
     * @param retrySpec    Reactor 的 Retry 规范。传入 null 可恢复使用节点默认值。
     * @return this builder。
     * @throws IllegalStateException 如果节点实例尚未定义。
     */
    public DagDefinitionBuilder<C> withRetry(String instanceName, Retry retrySpec) {
        NodeDefinition nodeDef = getNodeDefinitionOrThrow(instanceName);
        // 使用 StandardNodeExecutor 中定义的键存储
        if (retrySpec != null) {
            nodeDef.getConfigurationInternal().put(StandardNodeExecutor.CONFIG_KEY_RETRY, retrySpec);
            log.debug("DAG '{}': 为实例 '{}' 配置了自定义 Retry", dagName, instanceName);
        } else {
            // 允许移除自定义配置
            nodeDef.getConfigurationInternal().remove(StandardNodeExecutor.CONFIG_KEY_RETRY);
            log.debug("DAG '{}': 移除了实例 '{}' 的自定义 Retry (将使用节点默认值)", dagName, instanceName);
        }
        return this;
    }

    /**
     * 为特定节点实例设置自定义执行超时。
     * 这将覆盖节点实现中定义的默认超时或全局默认超时。
     *
     * @param instanceName 要配置的节点实例的名称。
     * @param timeout      自定义超时时长。不能为空或负数。
     * @return this builder。
     * @throws IllegalStateException 如果节点实例尚未定义。
     * @throws IllegalArgumentException 如果 timeout 为 null 或负数。
     */
    public DagDefinitionBuilder<C> withTimeout(String instanceName, Duration timeout) {
        Objects.requireNonNull(timeout, "实例 " + instanceName + " 的 Timeout 不能为空");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("实例 " + instanceName + " 的 Timeout 不能为负数");
        }
        NodeDefinition nodeDef = getNodeDefinitionOrThrow(instanceName);
        // 使用 StandardNodeExecutor 中定义的键存储
        nodeDef.getConfigurationInternal().put(StandardNodeExecutor.CONFIG_KEY_TIMEOUT, timeout);
        log.debug("DAG '{}': 为实例 '{}' 配置了自定义 Timeout: {}", dagName, instanceName, timeout);
        return this;
    }

    /**
     * 向特定节点实例添加通用的配置键值对。
     * 避免使用可能与内部配置键（如重试/超时的键）冲突的键。
     *
     * @param instanceName 要配置的节点实例的名称。
     * @param key          配置键。
     * @param value        配置值。
     * @return this builder。
     * @throws IllegalStateException 如果节点实例尚未定义。
     */
    public DagDefinitionBuilder<C> withConfiguration(String instanceName, String key, Object value) {
        Objects.requireNonNull(key, "实例 " + instanceName + " 的配置键不能为空");
        if (key.equals(StandardNodeExecutor.CONFIG_KEY_RETRY) || key.equals(StandardNodeExecutor.CONFIG_KEY_TIMEOUT)) {
            log.warn("DAG '{}': 在实例 '{}' 上为保留键 '{}' 使用了通用的 withConfiguration。考虑使用 withRetry/withTimeout 方法。", dagName, key, instanceName);
        }
        NodeDefinition nodeDef = getNodeDefinitionOrThrow(instanceName);
        nodeDef.getConfigurationInternal().put(key, value);
        log.debug("DAG '{}': 为实例 '{}' 添加了配置: {}={}", dagName, instanceName, key, value);
        return this;
    }

    // --- 边定义方法 (逻辑不变, 确保验证使用注册表) ---

    public DagDefinitionBuilder<C> addEdge(String upstreamInstanceName, String outputSlotId,
                                           String downstreamInstanceName, String inputSlotId,
                                           Condition<C> condition) {
        Objects.requireNonNull(upstreamInstanceName, "上游实例名称不能为空");
        Objects.requireNonNull(outputSlotId, "输出槽 ID 不能为空");
        Objects.requireNonNull(downstreamInstanceName, "下游实例名称不能为空");
        Objects.requireNonNull(inputSlotId, "输入槽 ID 不能为空");

        NodeDefinition upstreamDef = getNodeDefinitionOrThrow(upstreamInstanceName);
        NodeDefinition downstreamDef = getNodeDefinitionOrThrow(downstreamInstanceName);

        // 从注册表获取元数据进行验证
        NodeRegistry.NodeMetadata upstreamMeta = nodeRegistry.getNodeMetadata(upstreamDef.getNodeTypeId())
                .orElseThrow(() -> new IllegalStateException("上游节点类型 '" + upstreamDef.getNodeTypeId() + "' 的元数据在注册表中未找到。"));
        NodeRegistry.NodeMetadata downstreamMeta = nodeRegistry.getNodeMetadata(downstreamDef.getNodeTypeId())
                .orElseThrow(() -> new IllegalStateException("下游节点类型 '" + downstreamDef.getNodeTypeId() + "' 的元数据在注册表中未找到。"));

        // 验证插槽存在且类型兼容
        OutputSlot<?> outputSlot = upstreamMeta.findOutputSlot(outputSlotId)
                .orElseThrow(() -> new IllegalArgumentException(String.format("在节点类型 '%s' (实例: '%s') 上找不到输出槽 '%s'。",
                        outputSlotId, upstreamDef.getNodeTypeId(), upstreamInstanceName)));

        InputSlot<?> inputSlot = downstreamMeta.findInputSlot(inputSlotId)
                .orElseThrow(() -> new IllegalArgumentException(String.format("在节点类型 '%s' (实例: '%s') 上找不到输入槽 '%s'。",
                        inputSlotId, downstreamDef.getNodeTypeId(), downstreamInstanceName)));

        if (!inputSlot.getType().isAssignableFrom(outputSlot.getType())) {
            throw new IllegalArgumentException(String.format("边 %s[%s] -> %s[%s] 类型不匹配。需要输入类型: %s, 提供输出类型: %s。",
                    upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId,
                    inputSlot.getType().getName(), outputSlot.getType().getName()));
        }

        // 检查重复边（相同的源槽到相同的目标槽）
        boolean edgeExists = edgeDefinitions.stream().anyMatch(e ->
                e.getUpstreamInstanceName().equals(upstreamInstanceName) &&
                        e.getOutputSlotId().equals(outputSlotId) &&
                        e.getDownstreamInstanceName().equals(downstreamInstanceName) &&
                        e.getInputSlotId().equals(inputSlotId));
        if (edgeExists) {
            // 如果条件不同，是否允许重复边？目前不允许完全重复。
            log.warn("DAG '{}': 检测到重复边 (相同的源/目标槽): {}[{}] -> {}[{}]. 忽略添加。",
                    dagName, upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId);
            return this; // 或者抛出异常
        }


        EdgeDefinition<C> edgeDef = new EdgeDefinition<>(upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, condition);
        edgeDefinitions.add(edgeDef);
        log.debug("DAG '{}': 添加了边 {}[{}] -> {}[{}] {}", dagName, upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, condition != null ? "带条件" : "无条件");
        return this;
    }

    public DagDefinitionBuilder<C> addEdge(String upstreamInstanceName, String outputSlotId,
                                           String downstreamInstanceName, String inputSlotId) {
        return addEdge(upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, null);
    }

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
     * 执行图验证和拓扑排序。
     * 成功构建后，会生成并打印 DOT 图形描述代码。
     *
     * @return 不可变的 DagDefinition 实例。
     * @throws IllegalStateException 如果图验证失败（例如，循环、缺少必需输入）。
     */
    public DagDefinition<C> build() {
        log.info("开始为 '{}' 构建 DagDefinition...", dagName);

        // 在验证和最终构建之前，使节点配置不可变
        Map<String, NodeDefinition> finalNodeDefinitions = new LinkedHashMap<>();
        for (Map.Entry<String, NodeDefinition> entry : nodeDefinitions.entrySet()) {
            finalNodeDefinitions.put(entry.getKey(), entry.getValue().makeImmutable());
        }
        // 创建边的不可变副本，以防万一
        List<EdgeDefinition<C>> finalEdgeDefinitions = Collections.unmodifiableList(new ArrayList<>(edgeDefinitions));


        // 验证图结构 (需要 NodeRegistry 提供元数据)
        try {
            GraphUtils.validateGraphStructure(finalNodeDefinitions, finalEdgeDefinitions, nodeRegistry, dagName);
        } catch (IllegalStateException e) {
            log.error("DAG '{}' 构建期间验证失败: {}", dagName, e.getMessage());
            throw e;
        }

        // 检测循环
        try {
            GraphUtils.detectCycles(finalNodeDefinitions.keySet(), finalEdgeDefinitions, dagName);
        } catch (IllegalStateException e) {
            log.error("DAG '{}' 构建期间循环检测失败: {}", dagName, e.getMessage());
            throw e;
        }

        // 计算拓扑排序
        List<String> executionOrder;
        try {
            if (finalNodeDefinitions.isEmpty()) {
                executionOrder = Collections.emptyList();
            } else {
                executionOrder = GraphUtils.topologicalSort(finalNodeDefinitions.keySet(), finalEdgeDefinitions, dagName);
            }
        } catch (IllegalStateException e) {
            log.error("DAG '{}' 构建期间拓扑排序失败: {}", dagName, e.getMessage());
            throw e;
        }

        log.info("DAG '{}' 构建成功。{} 个节点, {} 条边。执行顺序: {}", dagName, finalNodeDefinitions.size(), finalEdgeDefinitions.size(), executionOrder);
        printDagStructure(finalNodeDefinitions); // 打印文本结构

        // --- 新增：生成并打印 DOT 代码 ---
        try {
            String dotCode = generateDotRepresentation(finalNodeDefinitions, finalEdgeDefinitions);
            log.info("DAG '{}' DOT 图形描述:\n--- DOT BEGIN ---\n{}\n--- DOT END ---", dagName, dotCode);
        } catch (Exception e) {
            // 捕获生成 DOT 代码时可能发生的任何异常，避免影响 build() 的主要流程
            log.error("DAG '{}': 生成 DOT 图形描述时发生错误: {}", dagName, e.getMessage(), e);
        }
        // --- 结束新增 ---

        // 创建不可变的 DagDefinition 实例
        return new DefaultDagDefinition<>(
                dagName,
                contextType,
                errorStrategy,
                finalNodeDefinitions, // 已经是不可变 Map
                finalEdgeDefinitions, // 已经是不可变 List
                executionOrder        // 已经是不可变列表
        );
    }

    // --- 辅助方法 ---

    private NodeDefinition getNodeDefinitionOrThrow(String instanceName) {
        NodeDefinition nodeDef = nodeDefinitions.get(instanceName);
        if (nodeDef == null) {
            throw new IllegalStateException(String.format("节点实例 '%s' 尚未通过 addNode() 定义。无法配置。", instanceName));
        }
        return nodeDef;
    }

    private void printDagStructure(Map<String, NodeDefinition> finalNodeDefs) {
        if (!log.isInfoEnabled() || finalNodeDefs.isEmpty()) {
            return;
        }
        log.info("DAG '{}' 最终结构 (文本):", dagName);
        StringBuilder builder = new StringBuilder("\n节点:\n");
        builder.append(String.format("%-30s | %-40s | %s\n", "实例名称", "类型 ID", "配置"));
        builder
//                .append("-".repeat(100))
                .append("\n");
        finalNodeDefs.forEach((name, def) -> builder.append(String.format("%-30s | %-40s | %s\n", name, def.getNodeTypeId(), def.getConfiguration()))); // getConfiguration 返回不可变 Map

        builder.append("\n边:\n");
        builder.append(String.format("%-30s [%-20s] ---> %-30s [%-20s] | %s\n", "上游", "输出槽", "下游", "输入槽", "条件"));
        builder
//                .append("-".repeat(120))
                .append("\n");
        edgeDefinitions.forEach(edge -> builder.append(String.format("%-30s [%-20s] ---> %-30s [%-20s] | %s\n",
                edge.getUpstreamInstanceName(), edge.getOutputSlotId(),
                edge.getDownstreamInstanceName(), edge.getInputSlotId(),
                edge.getCondition() == Condition.alwaysTrue() ? "总是 True" : edge.getCondition().getClass().getSimpleName())));

        log.info(builder.toString());
    }

    /**
     * 生成 DAG 的 DOT 图形描述字符串。
     *
     * @param nodeDefs 最终的节点定义 Map
     * @param edgeDefs 最终的边定义 List
     * @return DOT 格式的字符串
     */
    private String generateDotRepresentation(Map<String, NodeDefinition> nodeDefs, List<EdgeDefinition<C>> edgeDefs) {
        StringBuilder dot = new StringBuilder();
        String safeDagName = escapeDotString(dagName); // 对 DAG 名称进行转义

        dot.append(String.format("digraph \"%s\" {\n", safeDagName));
        dot.append("  rankdir=LR; // 从左到右布局\n");
        dot.append(String.format("  label=\"%s\";\n", safeDagName));
        dot.append("  node [shape=box, style=rounded];\n"); // 默认节点样式

        // 定义节点
        for (NodeDefinition node : nodeDefs.values()) {
            String instanceName = escapeDotString(node.getInstanceName());
            String typeId = escapeDotString(node.getNodeTypeId());
            // 节点标签：实例名 + 类型ID
            String nodeLabel = String.format("%s\\n(%s)", instanceName, typeId);
            dot.append(String.format("  \"%s\" [label=\"%s\"];\n", instanceName, nodeLabel));
        }

        // 定义边
        for (EdgeDefinition<C> edge : edgeDefs) {
            String upName = escapeDotString(edge.getUpstreamInstanceName());
            String downName = escapeDotString(edge.getDownstreamInstanceName());
            String outSlot = escapeDotString(edge.getOutputSlotId());
            String inSlot = escapeDotString(edge.getInputSlotId());
            Condition<C> condition = edge.getCondition();

            // 边标签：输出槽 -> 输入槽
            String edgeLabel = String.format("%s -> %s", outSlot, inSlot);
            List<String> attributes = new ArrayList<>();
            attributes.add(String.format("label=\"%s\"", edgeLabel));

            // 如果有条件，添加条件信息到标签，并使用虚线样式
            if (condition != Condition.alwaysTrue()) {
                String conditionName = escapeDotString(condition.getClass().getSimpleName());
                // 更新标签以包含条件
                edgeLabel = String.format("%s\\n[C: %s]", edgeLabel, conditionName);
                attributes.set(0, String.format("label=\"%s\"", edgeLabel)); // 替换原标签
                attributes.add("style=dashed");
                attributes.add("color=blue"); // 可选：用颜色区分条件边
            }

            dot.append(String.format("  \"%s\" -> \"%s\" [%s];\n",
                    upName, downName, String.join(", ", attributes)));
        }

        dot.append("}\n");
        return dot.toString();
    }

    /**
     * 对字符串进行转义，使其在 DOT 标签或 ID 中安全使用。
     * 主要处理双引号和反斜杠。
     *
     * @param input 输入字符串
     * @return 转义后的字符串
     */
    private String escapeDotString(String input) {
        if (input == null) {
            return "";
        }
        // 替换反斜杠为双反斜杠，替换双引号为反斜杠+双引号
        return input.replace("\\", "\\\\").replace("\"", "\\\"");
    }


    // 内部类，表示最终的不可变 DAG 定义实现
    // (从先前版本复制，确保使用最终的不可变 maps/lists)
    private static class DefaultDagDefinition<C> implements DagDefinition<C> {
        private final String dagName;
        private final Class<C> contextType;
        private final ErrorHandlingStrategy errorStrategy;
        private final Map<String, NodeDefinition> nodeDefinitions; // InstanceName -> NodeDefinition (应为不可变)
        private final List<EdgeDefinition<C>> edgeDefinitions;     // (应为不可变)
        private final List<String> executionOrder;                 // (应为不可变)
        private final Set<String> allNodeNames;
        private final Map<String, List<EdgeDefinition<C>>> incomingEdgesMap;
        private final Map<String, List<EdgeDefinition<C>>> outgoingEdgesMap;


        DefaultDagDefinition(String dagName, Class<C> contextType, ErrorHandlingStrategy errorStrategy,
                             Map<String, NodeDefinition> nodeDefinitions, // 传递最终的不可变 map
                             List<EdgeDefinition<C>> edgeDefinitions,     // 传递最终的不可变 list
                             List<String> executionOrder) {
            this.dagName = dagName;
            this.contextType = contextType;
            this.errorStrategy = errorStrategy;
            this.nodeDefinitions = nodeDefinitions; // 已经是不可变
            this.edgeDefinitions = edgeDefinitions; // 已经是不可变
            this.executionOrder = executionOrder;   // 已经是不可变
            this.allNodeNames = Collections.unmodifiableSet(nodeDefinitions.keySet());

            // 预计算边的查找
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
