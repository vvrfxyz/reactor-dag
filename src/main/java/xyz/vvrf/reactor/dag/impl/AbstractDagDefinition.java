// [文件名称]: AbstractDagDefinition.java
package xyz.vvrf.reactor.dag.impl;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.DagNodeDefinition;
import xyz.vvrf.reactor.dag.core.EdgeDefinition; // 引入 EdgeDefinition

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * DagDefinition 的抽象基类，提供通用的节点管理、验证和拓扑排序逻辑。
 * 使用 DagNodeDefinition 来描述节点实例，依赖关系通过 EdgeDefinition 定义。
 * 强制要求 DAG 内的节点名称必须唯一。
 *
 * @param <C> 上下文类型
 */
@Slf4j
public abstract class AbstractDagDefinition<C> implements DagDefinition<C> {

    private final Map<String, DagNodeDefinition<C, ?>> nodeDefinitionsByName = new ConcurrentHashMap<>();

    @Getter
    private List<String> executionOrder = Collections.emptyList();

    private final Class<C> contextType;
    private boolean initialized = false;
    private final String dagName;

    protected AbstractDagDefinition(Class<C> contextType, String dagName) {
        this.contextType = Objects.requireNonNull(contextType, "上下文类型不能为空");
        this.dagName = Objects.requireNonNull(dagName, "DAG 名称不能为空");
        log.info("为上下文 {} 创建 DAG 定义 '{}'...", contextType.getSimpleName(), dagName);
    }

    @Override
    public String getDagName() {
        return this.dagName;
    }

    @Override
    public Class<C> getContextType() {
        return this.contextType;
    }

    /**
     * 添加一个配置好的节点定义到 DAG 中。
     */
    public synchronized void addNodeDefinition(DagNodeDefinition<C, ?> nodeDefinition) {
        Objects.requireNonNull(nodeDefinition, "节点定义不能为空");
        String nodeName = nodeDefinition.getNodeName();

        if (initialized) {
            log.error("[{}] DAG '{}': 尝试在初始化后添加节点 '{}'，操作被拒绝。",
                    contextType.getSimpleName(), getDagName(), nodeName);
            throw new IllegalStateException("DAG 初始化后无法添加节点定义。");
        }

        if (nodeDefinitionsByName.containsKey(nodeName)) {
            DagNodeDefinition<C, ?> existingDef = nodeDefinitionsByName.get(nodeName);
            String errorMsg = String.format(
                    "节点名称冲突！名称 '%s' 已被逻辑 '%s' 使用，不能再被逻辑 '%s' 使用。",
                    nodeName,
                    existingDef.getNodeLogic().getLogicIdentifier(),
                    nodeDefinition.getNodeLogic().getLogicIdentifier()
            );
            log.error("[{}] DAG '{}': {}", contextType.getSimpleName(), getDagName(), errorMsg);
            throw new IllegalStateException(errorMsg);
        }

        nodeDefinitionsByName.put(nodeName, nodeDefinition);
        log.info("[{}] DAG '{}': 已添加节点定义: '{}' (逻辑: {}, 事件类型: {}, 依赖: {})",
                contextType.getSimpleName(), getDagName(), nodeName,
                nodeDefinition.getNodeLogic().getLogicIdentifier(),
                nodeDefinition.getEventType().getSimpleName(),
                nodeDefinition.getDependencyNames()); // 日志仍然显示依赖名称
    }


    @Override
    public synchronized void initialize() throws IllegalStateException {
        if (initialized) {
            log.debug("[{}] DAG '{}' 已初始化，跳过", contextType.getSimpleName(), getDagName());
            return;
        }
        if (nodeDefinitionsByName.isEmpty()) {
            log.warn("[{}] DAG '{}' 为空 (无节点定义)，初始化完成。", contextType.getSimpleName(), getDagName());
            this.executionOrder = Collections.emptyList();
            this.initialized = true;
            return;
        }

        log.info("[{}] 开始初始化和验证 DAG '{}'...", contextType.getSimpleName(), getDagName());
        try {
            validateDependencies(); // 验证依赖是否存在
            detectCycles();       // 检测循环依赖
            this.executionOrder = calculateExecutionOrder(); // 计算拓扑排序
            this.initialized = true;

            log.info("[{}] DAG '{}' 初始化成功", contextType.getSimpleName(), getDagName());
            log.info("[{}] DAG '{}' 执行顺序: {}", contextType.getSimpleName(), getDagName(), executionOrder);
            printDagStructure(); // 打印结构
            generateDotGraph();  // 生成 DOT 图
        } catch (IllegalStateException e) {
            log.error("[{}] DAG '{}' 初始化失败: {}", contextType.getSimpleName(), getDagName(), e.getMessage());
            this.executionOrder = Collections.emptyList();
            this.initialized = false;
            throw e;
        }
    }

    private void validateDependencies() {
        log.debug("[{}] DAG '{}': 验证依赖关系...", contextType.getSimpleName(), getDagName());
        for (DagNodeDefinition<C, ?> nodeDef : nodeDefinitionsByName.values()) {
            String nodeName = nodeDef.getNodeName();
            // 遍历所有入边
            for (EdgeDefinition<C> edge : nodeDef.getIncomingEdges()) {
                String depName = edge.getDependencyNodeName();
                // 检查依赖的节点是否存在 (基于名称)
                if (!nodeDefinitionsByName.containsKey(depName)) {
                    String errorMsg = String.format("节点 '%s' (逻辑: %s) 的入边定义了不存在的依赖节点 '%s'",
                            nodeName, nodeDef.getNodeLogic().getLogicIdentifier(), depName);
                    throw new IllegalStateException(String.format("[%s] DAG '%s': %s", contextType.getSimpleName(), getDagName(), errorMsg));
                }
            }
        }
        log.info("[{}] DAG '{}': 依赖关系验证通过", contextType.getSimpleName(), getDagName());
    }

    private void detectCycles() {
        log.debug("[{}] DAG '{}': 检测循环依赖...", contextType.getSimpleName(), getDagName());
        Set<String> visited = new HashSet<>();
        Set<String> visiting = new HashSet<>();

        for (String nodeName : nodeDefinitionsByName.keySet()) {
            if (!visited.contains(nodeName)) {
                checkCycleDFS(nodeName, visiting, visited);
            }
        }
        log.info("[{}] DAG '{}': 未检测到循环依赖", contextType.getSimpleName(), getDagName());
    }

    private void checkCycleDFS(String nodeName, Set<String> visiting, Set<String> visited) {
        visiting.add(nodeName);

        DagNodeDefinition<C, ?> nodeDef = nodeDefinitionsByName.get(nodeName);
        if (nodeDef == null) {
            throw new IllegalStateException(String.format("[%s] DAG '%s': 在循环检测中遇到未注册的节点 '%s'",
                    contextType.getSimpleName(), getDagName(), nodeName));
        }

        // 获取依赖节点名称列表
        List<String> dependencies = nodeDef.getDependencyNames();

        if (!dependencies.isEmpty()) {
            for (String depName : dependencies) {
                // 再次确认依赖存在 (validateDependencies 应该已保证)
                if (!nodeDefinitionsByName.containsKey(depName)) {
                    throw new IllegalStateException(String.format("[%s] DAG '%s': 节点 '%s' 依赖了不存在的节点 '%s' (在循环检测中发现)",
                            contextType.getSimpleName(), getDagName(), nodeName, depName));
                }

                if (visiting.contains(depName)) {
                    throw new IllegalStateException(
                            String.format("[%s] DAG '%s': 检测到循环依赖！路径涉及 '%s' -> '%s' (可能更长)",
                                    contextType.getSimpleName(), getDagName(), depName, nodeName));
                }

                if (!visited.contains(depName)) {
                    checkCycleDFS(depName, visiting, visited);
                }
            }
        }

        visiting.remove(nodeName);
        visited.add(nodeName);
    }

    private List<String> calculateExecutionOrder() {
        log.debug("[{}] DAG '{}': 计算拓扑执行顺序...", contextType.getSimpleName(), getDagName());
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> adj = new HashMap<>(); // key: 依赖节点, value: 依赖于 key 的节点列表

        for (String nodeName : nodeDefinitionsByName.keySet()) {
            inDegree.put(nodeName, 0);
            adj.put(nodeName, new ArrayList<>());
        }

        // 构建图和计算入度
        for (DagNodeDefinition<C, ?> nodeDef : nodeDefinitionsByName.values()) {
            String dependerName = nodeDef.getNodeName(); // 依赖者
            // 遍历所有入边来确定依赖关系
            for (EdgeDefinition<C> edge : nodeDef.getIncomingEdges()) {
                String dependencyName = edge.getDependencyNodeName(); // 被依赖者
                // 添加边: dependencyName -> dependerName
                if (adj.containsKey(dependencyName)) {
                    adj.get(dependencyName).add(dependerName);
                    // 增加依赖者的入度
                    inDegree.put(dependerName, inDegree.get(dependerName) + 1);
                } else {
                    throw new IllegalStateException(String.format("[%s] DAG '%s': 在拓扑排序中发现未经验证的依赖 '%s' -> '%s'",
                            contextType.getSimpleName(), getDagName(), dependencyName, dependerName));
                }
            }
        }

        // Kahn 算法
        Queue<String> queue = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.offer(entry.getKey());
            }
        }

        List<String> sortedOrder = new ArrayList<>();
        while (!queue.isEmpty()) {
            String u = queue.poll();
            sortedOrder.add(u);

            for (String v : adj.get(u)) {
                inDegree.put(v, inDegree.get(v) - 1);
                if (inDegree.get(v) == 0) {
                    queue.offer(v);
                }
            }
        }

        if (sortedOrder.size() != nodeDefinitionsByName.size()) {
            Set<String> remainingNodes = nodeDefinitionsByName.keySet().stream()
                    .filter(n -> !sortedOrder.contains(n))
                    .collect(Collectors.toSet());
            log.error("[{}] DAG '{}': 拓扑排序失败，图中可能存在循环或节点不可达。未排序节点: {}",
                    contextType.getSimpleName(), getDagName(), remainingNodes);
            throw new IllegalStateException(String.format("[%s] DAG '%s': 拓扑排序失败，检测到循环或节点不可达。未排序节点: %s",
                    contextType.getSimpleName(), getDagName(), remainingNodes));
        }

        return Collections.unmodifiableList(sortedOrder);
    }

    private void printDagStructure() {
        if (!log.isInfoEnabled() || nodeDefinitionsByName.isEmpty()) {
            return;
        }

        log.info("[{}] DAG '{}' 结构表示:", contextType.getSimpleName(), getDagName());
        StringBuilder builder = new StringBuilder("\n");
        builder.append(String.format("%-40s | %-40s | %-30s | %-40s | %-40s\n",
                "节点名称", "逻辑实现", "事件类型", "依赖节点", "被依赖节点"));
        StringBuilder divider = new StringBuilder();
        for (int i = 0; i < 195; i++) { divider.append("-"); }
        builder.append(divider).append("\n");

        Map<String, Set<String>> dependsOn = new HashMap<>();
        Map<String, Set<String>> dependedBy = new HashMap<>();

        for (String nodeName : nodeDefinitionsByName.keySet()) {
            dependsOn.put(nodeName, new HashSet<>());
            dependedBy.put(nodeName, new HashSet<>());
        }

        // 填充依赖关系 (基于 EdgeDefinition)
        for (DagNodeDefinition<C, ?> nodeDef : nodeDefinitionsByName.values()) {
            String nodeName = nodeDef.getNodeName();
            for (EdgeDefinition<C> edge : nodeDef.getIncomingEdges()) {
                String depName = edge.getDependencyNodeName();
                dependsOn.computeIfAbsent(nodeName, k -> new HashSet<>()).add(depName);
                dependedBy.computeIfAbsent(depName, k -> new HashSet<>()).add(nodeName);
            }
        }

        List<String> nodesToIterate = executionOrder.isEmpty()
                ? new ArrayList<>(nodeDefinitionsByName.keySet())
                : executionOrder;
        if (executionOrder.isEmpty() && !nodeDefinitionsByName.isEmpty()) {
            Collections.sort(nodesToIterate);
        }

        if (nodesToIterate.isEmpty()) {
            builder.append("  (DAG 为空)\n");
        } else {
            for (String nodeName : nodesToIterate) {
                DagNodeDefinition<C, ?> nodeDef = nodeDefinitionsByName.get(nodeName);
                if (nodeDef == null) {
                    continue;
                }

                String logicIdentifier = nodeDef.getNodeLogic().getLogicIdentifier();
                String eventType = nodeDef.getEventType().getSimpleName();
                String dependsOnStr = formatNodeSet(dependsOn.get(nodeName));
                String dependedByStr = formatNodeSet(dependedBy.get(nodeName));

                builder.append(String.format("%-40s | %-40s | %-30s | %-40s | %-40s\n",
                        nodeName, logicIdentifier, eventType, dependsOnStr, dependedByStr));
            }
        }

        if (!executionOrder.isEmpty()) {
            builder.append("\n执行路径 (→ 表示执行顺序):\n");
            builder.append(String.join(" → ", executionOrder));
        } else if (!nodeDefinitionsByName.isEmpty()) {
            builder.append("\n(无有效执行路径 - 可能存在循环、节点不可达或未初始化)\n");
        }

        log.info(builder.toString());
    }

    private void generateDotGraph() {
        if (!log.isInfoEnabled() || nodeDefinitionsByName.isEmpty()) {
            return;
        }

        StringBuilder dotGraph = new StringBuilder();
        String graphName = String.format("DAG_%s_%s", contextType.getSimpleName(), getDagName().replaceAll("\\W+", "_"));
        dotGraph.append(String.format("digraph %s {\n", graphName));
        dotGraph.append(String.format("  label=\"DAG Structure (%s - %s)\";\n", contextType.getSimpleName(), getDagName()));
        dotGraph.append("  labelloc=top;\n");
        dotGraph.append("  fontsize=16;\n");
        dotGraph.append("  rankdir=LR;\n");
        dotGraph.append("  node [shape=record, style=\"rounded,filled\", fillcolor=\"lightblue\", fontname=\"Arial\", fontsize=10];\n");
        dotGraph.append("  edge [fontname=\"Arial\", fontsize=9];\n");

        for (DagNodeDefinition<C, ?> nodeDef : nodeDefinitionsByName.values()) {
            String nodeName = nodeDef.getNodeName();
            String logicId = nodeDef.getNodeLogic().getLogicIdentifier();
            String eventType = nodeDef.getEventType().getSimpleName();
            String label = String.format("{%s | Logic: %s | Event: %s}",
                    nodeName.replace("\"", "\\\""),
                    logicId.replace("\"", "\\\""),
                    eventType.replace("\"", "\\\""));
            dotGraph.append(String.format("  \"%s\" [label=\"%s\"];\n", nodeName, label));
        }

        // 定义边 (基于 EdgeDefinition)
        for (DagNodeDefinition<C, ?> nodeDef : nodeDefinitionsByName.values()) {
            String dependerName = nodeDef.getNodeName();
            for (EdgeDefinition<C> edge : nodeDef.getIncomingEdges()) {
                String dependencyName = edge.getDependencyNodeName();
                if (nodeDefinitionsByName.containsKey(dependencyName)) {
                    // 可以选择性地在边上添加标签指示条件类型
                    boolean isDefaultCond = edge.isDefaultCondition();
                    String edgeLabel = isDefaultCond ? "" : " [label=\"Custom\"]"; // 示例
                    dotGraph.append(String.format("  \"%s\" -> \"%s\"%s;\n", dependencyName, dependerName, edgeLabel));
                }
            }
        }
        dotGraph.append("}\n");
        log.info("[{}] DAG '{}' DOT格式图 (可使用 Graphviz 查看):\n{}", contextType.getSimpleName(), getDagName(), dotGraph.toString());
    }


    private String formatNodeSet(Set<String> nodeSet) {
        if (nodeSet == null || nodeSet.isEmpty()) {
            return "无";
        }
        List<String> sortedNodes = new ArrayList<>(nodeSet);
        Collections.sort(sortedNodes);
        return String.join(", ", sortedNodes);
    }

    @Override
    public Optional<DagNodeDefinition<C, ?>> getNodeDefinition(String nodeName) {
        return Optional.ofNullable(nodeDefinitionsByName.get(nodeName));
    }

    @Override
    public Collection<DagNodeDefinition<C, ?>> getAllNodeDefinitions() {
        return Collections.unmodifiableCollection(nodeDefinitionsByName.values());
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }
}
