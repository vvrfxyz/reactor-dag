package xyz.vvrf.reactor.dag.builder;

import lombok.extern.slf4j.Slf4j;
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.execution.StandardNodeExecutor;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;
import xyz.vvrf.reactor.dag.util.GraphUtils;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 用于以编程方式构建不可变的 DagDefinition 数据结构。
 * 需要一个 NodeRegistry 来验证节点类型和插槽。
 * 允许实例特定的配置（重试、超时）。
 * 新增：构建成功后可生成 DOT 图形描述代码。
 *
 * @param <C> 上下文类型。
 * @author ruifeng.wen
 */
@Slf4j
public class DagDefinitionBuilder<C> {

    private final Class<C> contextType;
    private final NodeRegistry<C> nodeRegistry;
    private String dagName;
    private ErrorHandlingStrategy errorStrategy = ErrorHandlingStrategy.FAIL_FAST;

    private final Map<String, NodeDefinition> nodeDefinitions = new LinkedHashMap<>();
    private final List<EdgeDefinition<C>> edgeDefinitions = new ArrayList<>();

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

    public DagDefinitionBuilder<C> addNode(String instanceName, String nodeTypeId) {
        Objects.requireNonNull(instanceName, "实例名称不能为空");
        Objects.requireNonNull(nodeTypeId, "节点类型 ID 不能为空");

        if (nodeDefinitions.containsKey(instanceName)) {
            throw new IllegalArgumentException(String.format("节点实例名称 '%s' 在 DAG '%s' 中已存在。", instanceName, dagName));
        }
        if (!nodeRegistry.getNodeMetadata(nodeTypeId).isPresent()) {
            throw new IllegalArgumentException(String.format("在 NodeRegistry (上下文: '%s') 中找不到节点类型 ID '%s'。无法为 DAG '%s' 添加实例 '%s'。",
                    nodeTypeId, contextType.getSimpleName(), instanceName, dagName));
        }

        NodeDefinition nodeDef = new NodeDefinition(instanceName, nodeTypeId, new HashMap<>());
        nodeDefinitions.put(instanceName, nodeDef);
        log.debug("DAG '{}': 添加了节点定义 '{}' (类型: {})", dagName, instanceName, nodeTypeId);
        return this;
    }

    public DagDefinitionBuilder<C> withRetry(String instanceName, Retry retrySpec) {
        NodeDefinition nodeDef = getNodeDefinitionOrThrow(instanceName);
        if (retrySpec != null) {
            nodeDef.getConfigurationInternal().put(StandardNodeExecutor.CONFIG_KEY_RETRY, retrySpec);
            log.debug("DAG '{}': 为实例 '{}' 配置了自定义 Retry", dagName, instanceName);
        } else {
            nodeDef.getConfigurationInternal().remove(StandardNodeExecutor.CONFIG_KEY_RETRY);
            log.debug("DAG '{}': 移除了实例 '{}' 的自定义 Retry (将使用节点默认值)", dagName, instanceName);
        }
        return this;
    }

    public DagDefinitionBuilder<C> withTimeout(String instanceName, Duration timeout) {
        Objects.requireNonNull(timeout, "实例 " + instanceName + " 的 Timeout 不能为空");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("实例 " + instanceName + " 的 Timeout 不能为负数");
        }
        NodeDefinition nodeDef = getNodeDefinitionOrThrow(instanceName);
        nodeDef.getConfigurationInternal().put(StandardNodeExecutor.CONFIG_KEY_TIMEOUT, timeout);
        log.debug("DAG '{}': 为实例 '{}' 配置了自定义 Timeout: {}", dagName, instanceName, timeout);
        return this;
    }

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

    public DagDefinitionBuilder<C> addEdge(String upstreamInstanceName, String outputSlotId,
                                           String downstreamInstanceName, String inputSlotId,
                                           ConditionBase<C> condition) {
        Objects.requireNonNull(upstreamInstanceName, "上游实例名称不能为空");
        Objects.requireNonNull(outputSlotId, "输出槽 ID 不能为空");
        Objects.requireNonNull(downstreamInstanceName, "下游实例名称不能为空");
        Objects.requireNonNull(inputSlotId, "输入槽 ID 不能为空");

        NodeDefinition upstreamDef = getNodeDefinitionOrThrow(upstreamInstanceName);
        NodeDefinition downstreamDef = getNodeDefinitionOrThrow(downstreamInstanceName);
        NodeRegistry.NodeMetadata upstreamMeta = nodeRegistry.getNodeMetadata(upstreamDef.getNodeTypeId())
                .orElseThrow(() -> new IllegalStateException("上游节点类型 '" + upstreamDef.getNodeTypeId() + "' 的元数据在注册表中未找到。"));
        NodeRegistry.NodeMetadata downstreamMeta = nodeRegistry.getNodeMetadata(downstreamDef.getNodeTypeId())
                .orElseThrow(() -> new IllegalStateException("下游节点类型 '" + downstreamDef.getNodeTypeId() + "' 的元数据在注册表中未找到。"));
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

        boolean edgeExists = edgeDefinitions.stream().anyMatch(e ->
                e.getUpstreamInstanceName().equals(upstreamInstanceName) &&
                        e.getOutputSlotId().equals(outputSlotId) &&
                        e.getDownstreamInstanceName().equals(downstreamInstanceName) &&
                        e.getInputSlotId().equals(inputSlotId));
        if (edgeExists) {
            log.warn("DAG '{}': 检测到重复边 (相同的源/目标槽): {}[{}] -> {}[{}]. 忽略添加。",
                    dagName, upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId);
            return this;
        }

        if (condition instanceof DeclaredDependencyCondition) {
            Set<String> declaredDeps = ((DeclaredDependencyCondition<C>) condition).getRequiredNodeDependencies();
            if (declaredDeps == null) {
                throw new IllegalArgumentException(String.format(
                        "边 %s -> %s 的 DeclaredDependencyCondition 不能返回 null 的依赖集合。",
                        upstreamInstanceName, downstreamInstanceName));
            }
            for (String depName : declaredDeps) {
                if (!nodeDefinitions.containsKey(depName)) {
                    throw new IllegalArgumentException(String.format(
                            "边 %s -> %s 的 DeclaredDependencyCondition 声明了不存在的节点依赖 '%s'。",
                            upstreamInstanceName, downstreamInstanceName, depName));
                }
                if (depName.equals(upstreamInstanceName)) {
                    log.warn("边 {} -> {} 的 DeclaredDependencyCondition 显式声明了对直接上游 '{}' 的依赖，这是不必要的。",
                            upstreamInstanceName, downstreamInstanceName, upstreamInstanceName);
                }
            }
        }

        EdgeDefinition<C> edgeDef = new EdgeDefinition<>(upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, condition);
        edgeDefinitions.add(edgeDef);
        log.debug("DAG '{}': 添加了边 {}[{}] -> {}[{}] (条件类型: {})", dagName, upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, edgeDef.getCondition().getClass().getSimpleName());
        return this;
    }

    public DagDefinitionBuilder<C> addEdge(String upstreamInstanceName, String outputSlotId,
                                           String downstreamInstanceName, String inputSlotId) {
        return addEdge(upstreamInstanceName, outputSlotId, downstreamInstanceName, inputSlotId, (ConditionBase<C>) null);
    }

    public DagDefinitionBuilder<C> addEdge(String upstreamInstanceName,
                                           String downstreamInstanceName, String inputSlotId,
                                           ConditionBase<C> condition) {
        return addEdge(upstreamInstanceName, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, downstreamInstanceName, inputSlotId, condition);
    }

    public DagDefinitionBuilder<C> addEdge(String upstreamInstanceName,
                                           String downstreamInstanceName, String inputSlotId) {
        return addEdge(upstreamInstanceName, OutputSlot.DEFAULT_OUTPUT_SLOT_ID, downstreamInstanceName, inputSlotId, (ConditionBase<C>) null);
    }

    public DagDefinition<C> build() {
        log.info("开始为 '{}' 构建 DagDefinition...", dagName);

        Map<String, NodeDefinition> finalNodeDefinitions = new LinkedHashMap<>();
        for (Map.Entry<String, NodeDefinition> entry : nodeDefinitions.entrySet()) {
            finalNodeDefinitions.put(entry.getKey(), entry.getValue().makeImmutable());
        }
        List<EdgeDefinition<C>> finalEdgeDefinitions = Collections.unmodifiableList(new ArrayList<>(edgeDefinitions));

        try {
            GraphUtils.validateGraphStructure(finalNodeDefinitions, finalEdgeDefinitions, nodeRegistry, dagName);
        } catch (IllegalStateException e) {
            log.error("DAG '{}' 构建期间验证失败: {}", dagName, e.getMessage());
            throw e;
        }

        try {
            GraphUtils.detectCycles(finalNodeDefinitions.keySet(), finalEdgeDefinitions, dagName);
        } catch (IllegalStateException e) {
            log.error("DAG '{}' 构建期间循环检测失败: {}", dagName, e.getMessage());
            throw e;
        }

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
        printDagStructure(finalNodeDefinitions);

        try {
            String dotCode = generateDotRepresentation(finalNodeDefinitions, finalEdgeDefinitions);
            log.info("DAG '{}' DOT 图形描述:\n--- DOT BEGIN ---\n{}\n--- DOT END ---", dagName, dotCode);
        } catch (Exception e) {
            log.error("DAG '{}': 生成 DOT 图形描述时发生错误: {}", dagName, e.getMessage(), e);
        }

        return new DefaultDagDefinition<>(
                dagName,
                contextType,
                errorStrategy,
                finalNodeDefinitions,
                finalEdgeDefinitions,
                executionOrder
        );
    }

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
        finalNodeDefs.forEach((name, def) -> builder.append(String.format("%-30s | %-40s | %s\n", name, def.getNodeTypeId(), def.getConfiguration())));

        builder.append("\n边:\n");
        builder.append(String.format("%-30s [%-20s] ---> %-30s [%-20s] | %s\n", "上游", "输出槽", "下游", "输入槽", "条件"));
        builder
//                .append("-".repeat(120))
                .append("\n");
        edgeDefinitions.forEach(edge -> builder.append(String.format("%-30s [%-20s] ---> %-30s [%-20s] | %s\n",
                edge.getUpstreamInstanceName(), edge.getOutputSlotId(),
                edge.getDownstreamInstanceName(), edge.getInputSlotId(),
                (edge.getCondition() instanceof DirectUpstreamCondition.AlwaysTrueCondition) ? "总是 True" : edge.getCondition().getClass().getSimpleName())));

        log.info(builder.toString());
    }

    private String generateDotRepresentation(Map<String, NodeDefinition> nodeDefs, List<EdgeDefinition<C>> edgeDefs) {
        StringBuilder dot = new StringBuilder();
        String safeDagName = escapeDotString(dagName);

        dot.append(String.format("digraph \"%s\" {\n", safeDagName));
        dot.append("  rankdir=LR;\n");
        dot.append(String.format("  label=\"%s\";\n", safeDagName));
        dot.append("  node [shape=box, style=rounded];\n");

        for (NodeDefinition node : nodeDefs.values()) {
            String instanceName = escapeDotString(node.getInstanceName());
            String typeId = escapeDotString(node.getNodeTypeId());
            String nodeLabel = String.format("%s\\n(%s)", instanceName, typeId);
            dot.append(String.format("  \"%s\" [label=\"%s\"];\n", instanceName, nodeLabel));
        }

        for (EdgeDefinition<C> edge : edgeDefs) {
            String upName = escapeDotString(edge.getUpstreamInstanceName());
            String downName = escapeDotString(edge.getDownstreamInstanceName());
            String outSlot = escapeDotString(edge.getOutputSlotId());
            String inSlot = escapeDotString(edge.getInputSlotId());
            ConditionBase<C> condition = edge.getCondition();

            String baseEdgeLabel = String.format("%s -> %s", outSlot, inSlot);
            List<String> attributes = new ArrayList<>();
            String finalEdgeLabel = baseEdgeLabel;

            if (condition instanceof DirectUpstreamCondition.AlwaysTrueCondition) {
                // No special styling
            } else if (condition instanceof DirectUpstreamCondition) {
                finalEdgeLabel = String.format("%s\\n(Direct)", baseEdgeLabel);
                attributes.add("color=darkgreen");
            } else if (condition instanceof LocalInputCondition) {
                finalEdgeLabel = String.format("%s\\n(Local)", baseEdgeLabel);
                attributes.add("style=dotted");
                attributes.add("color=orange");
            } else if (condition instanceof DeclaredDependencyCondition) {
                Set<String> deps = ((DeclaredDependencyCondition<C>) condition).getRequiredNodeDependencies();
                String depsStr = deps.stream().map(this::escapeDotString).collect(Collectors.joining(", "));
                finalEdgeLabel = String.format("%s\\n(Deps: %s)", baseEdgeLabel, depsStr.isEmpty() ? "None" : depsStr);
                attributes.add("style=dashed");
                attributes.add("color=blue");
            } else {
                finalEdgeLabel = String.format("%s\\n(Unknown Type)", baseEdgeLabel);
                attributes.add("color=red");
            }

            attributes.add(0, String.format("label=\"%s\"", finalEdgeLabel));

            dot.append(String.format("  \"%s\" -> \"%s\" [%s];\n",
                    upName, downName, String.join(", ", attributes)));
        }

        dot.append("}\n");
        return dot.toString();
    }

    private String escapeDotString(String input) {
        if (input == null) {
            return "";
        }
        return input.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static class DefaultDagDefinition<C> implements DagDefinition<C> {
        private final String dagName;
        private final Class<C> contextType;
        private final ErrorHandlingStrategy errorStrategy;
        private final Map<String, NodeDefinition> nodeDefinitions;
        private final List<EdgeDefinition<C>> edgeDefinitions;
        private final List<String> executionOrder;
        private final Set<String> allNodeNames;
        private final Map<String, List<EdgeDefinition<C>>> incomingEdgesMap;
        private final Map<String, List<EdgeDefinition<C>>> outgoingEdgesMap;

        DefaultDagDefinition(String dagName, Class<C> contextType, ErrorHandlingStrategy errorStrategy,
                             Map<String, NodeDefinition> nodeDefinitions,
                             List<EdgeDefinition<C>> edgeDefinitions,
                             List<String> executionOrder) {
            this.dagName = dagName;
            this.contextType = contextType;
            this.errorStrategy = errorStrategy;
            this.nodeDefinitions = nodeDefinitions;
            this.edgeDefinitions = edgeDefinitions;
            this.executionOrder = executionOrder;
            this.allNodeNames = Collections.unmodifiableSet(nodeDefinitions.keySet());
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
