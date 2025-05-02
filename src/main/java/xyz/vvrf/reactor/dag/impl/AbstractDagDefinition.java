// [file name]: AbstractDagDefinition.java
package xyz.vvrf.reactor.dag.impl;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.DagNodeDefinition; // 使用新的 Definition 类

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * DagDefinition 的抽象基类，提供通用的节点管理、验证和拓扑排序逻辑。
 * 使用 DagNodeDefinition 来描述节点实例。
 * 强制要求 DAG 内的节点名称必须唯一。
 *
 * @param <C> 上下文类型
 */
@Slf4j
public abstract class AbstractDagDefinition<C> implements DagDefinition<C> {

    // 存储节点定义，Key 是节点名称
    private final Map<String, DagNodeDefinition<C, ?>> nodeDefinitionsByName = new ConcurrentHashMap<>();

    @Getter
    private List<String> executionOrder = Collections.emptyList();

    private final Class<C> contextType;
    private boolean initialized = false;
    private final String dagName; // DAG 名称

    /**
     * 构造函数
     *
     * @param contextType 此 DAG 定义关联的上下文类型
     * @param dagName     DAG 的名称
     */
    protected AbstractDagDefinition(Class<C> contextType, String dagName) {
        this.contextType = Objects.requireNonNull(contextType, "上下文类型不能为空");
        this.dagName = Objects.requireNonNull(dagName, "DAG 名称不能为空");
        log.info("为上下文 {} 创建 DAG 定义 '{}'...", contextType.getSimpleName(), dagName);
    }

    @Override
    public String getDagName() {
        return this.dagName;
    }

    /**
     * 添加一个配置好的节点定义到 DAG 中。
     * 通常由 ChainBuilder 调用。
     *
     * @param nodeDefinition 要添加的节点定义。
     * @throws IllegalStateException 如果节点名称已存在。
     */
    public synchronized void addNodeDefinition(DagNodeDefinition<C, ?> nodeDefinition) {
        Objects.requireNonNull(nodeDefinition, "节点定义不能为空");
        String nodeName = nodeDefinition.getNodeName();

        if (initialized) {
            log.error("[{}] DAG '{}': 尝试在初始化后添加节点 '{}'，操作被拒绝。",
                    contextType.getSimpleName(), getDagName(), nodeName);
            throw new IllegalStateException("Cannot add node definition after DAG has been initialized.");
        }

        if (nodeDefinitionsByName.containsKey(nodeName)) {
            DagNodeDefinition<C, ?> existingDef = nodeDefinitionsByName.get(nodeName);
            String errorMsg = String.format(
                    "节点名称冲突！名称 '%s' 已被逻辑 '%s' 使用，不能再被逻辑 '%s' 使用。节点名称在 DAG 中必须唯一。",
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
                nodeDefinition.getDependencyNames());
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
            this.initialized = false; // 保持未初始化状态
            // 清理可能部分计算的状态？取决于实现，这里简单重置执行顺序
            nodeDefinitionsByName.clear(); // 如果初始化失败，清空节点定义可能更安全？或者保留？保留以供调试。
            throw e; // 重新抛出异常
        }
    }

    private void validateDependencies() {
        log.debug("[{}] DAG '{}': 验证依赖关系...", contextType.getSimpleName(), getDagName());
        for (DagNodeDefinition<C, ?> nodeDef : nodeDefinitionsByName.values()) {
            String nodeName = nodeDef.getNodeName();
            List<String> dependencyNames = nodeDef.getDependencyNames();

            if (dependencyNames.isEmpty()) {
                continue;
            }

            for (String depName : dependencyNames) {
                // 检查依赖的节点是否存在 (基于名称)
                if (!nodeDefinitionsByName.containsKey(depName)) {
                    String errorMsg = String.format("节点 '%s' (逻辑: %s) 依赖了不存在的节点 '%s'",
                            nodeName, nodeDef.getNodeLogic().getLogicIdentifier(), depName);
                    throw new IllegalStateException(String.format("[%s] DAG '%s': %s", contextType.getSimpleName(), getDagName(), errorMsg));
                }
                // Payload 类型检查已移除
            }
        }
        log.info("[{}] DAG '{}': 依赖关系验证通过", contextType.getSimpleName(), getDagName());
    }

    private void detectCycles() {
        log.debug("[{}] DAG '{}': 检测循环依赖...", contextType.getSimpleName(), getDagName());
        Set<String> visited = new HashSet<>(); // 完全访问过的节点
        Set<String> visiting = new HashSet<>(); // 当前递归路径上的节点

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
        // 防御性检查，理论上 validateDependencies 后不会为 null
        if (nodeDef == null) {
            throw new IllegalStateException(String.format("[%s] DAG '%s': 在循环检测中遇到未注册的节点 '%s'",
                    contextType.getSimpleName(), getDagName(), nodeName));
        }

        List<String> dependencies = nodeDef.getDependencyNames();

        if (!dependencies.isEmpty()) {
            for (String depName : dependencies) {
                // 再次确认依赖存在
                if (!nodeDefinitionsByName.containsKey(depName)) {
                    throw new IllegalStateException(String.format("[%s] DAG '%s': 节点 '%s' 依赖了不存在的节点 '%s' (在循环检测中发现)",
                            contextType.getSimpleName(), getDagName(), nodeName, depName));
                }

                if (visiting.contains(depName)) {
                    // 发现循环
                    throw new IllegalStateException(
                            String.format("[%s] DAG '%s': 检测到循环依赖！路径涉及 '%s' -> '%s' (可能更长)",
                                    contextType.getSimpleName(), getDagName(), depName, nodeName));
                }

                if (!visited.contains(depName)) {
                    checkCycleDFS(depName, visiting, visited);
                }
            }
        }

        visiting.remove(nodeName); // 离开当前路径
        visited.add(nodeName);     // 标记为已完全访问
    }

    private List<String> calculateExecutionOrder() {
        log.debug("[{}] DAG '{}': 计算拓扑执行顺序...", contextType.getSimpleName(), getDagName());
        Map<String, Integer> inDegree = new HashMap<>(); // 入度计数
        Map<String, List<String>> adj = new HashMap<>(); // 邻接表 (key: 依赖节点, value: 依赖于 key 的节点列表)

        // 初始化所有节点的入度和邻接表
        for (String nodeName : nodeDefinitionsByName.keySet()) {
            inDegree.put(nodeName, 0);
            adj.put(nodeName, new ArrayList<>());
        }

        // 构建图和计算入度
        for (DagNodeDefinition<C, ?> nodeDef : nodeDefinitionsByName.values()) {
            String dependerName = nodeDef.getNodeName(); // 依赖者
            List<String> dependencies = nodeDef.getDependencyNames(); // 被依赖者列表

            if (!dependencies.isEmpty()) {
                for (String dependencyName : dependencies) {
                    // 添加边: dependencyName -> dependerName
                    if (adj.containsKey(dependencyName)) {
                        adj.get(dependencyName).add(dependerName);
                        // 增加依赖者的入度
                        inDegree.put(dependerName, inDegree.get(dependerName) + 1);
                    } else {
                        // 理论上 validateDependencies 后不会发生
                        throw new IllegalStateException(String.format("[%s] DAG '%s': 在拓扑排序中发现未经验证的依赖 '%s' -> '%s'",
                                contextType.getSimpleName(), getDagName(), dependencyName, dependerName));
                    }
                }
            }
        }

        // Kahn 算法
        Queue<String> queue = new LinkedList<>();
        // 将所有入度为 0 的节点加入队列
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.offer(entry.getKey());
            }
        }

        List<String> sortedOrder = new ArrayList<>();
        while (!queue.isEmpty()) {
            String u = queue.poll(); // 取出入度为 0 的节点
            sortedOrder.add(u);

            // 遍历 u 的所有邻居节点 v
            for (String v : adj.get(u)) {
                // 将 v 的入度减 1
                inDegree.put(v, inDegree.get(v) - 1);
                // 如果 v 的入度变为 0，加入队列
                if (inDegree.get(v) == 0) {
                    queue.offer(v);
                }
            }
        }

        // 检查排序结果是否包含所有节点
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
        for (int i = 0; i < 195; i++) {
            divider.append("-"); // 调整分隔线长度
        }
        builder.append(divider).append("\n");

        Map<String, Set<String>> dependsOn = new HashMap<>(); // key 依赖 value 集合
        Map<String, Set<String>> dependedBy = new HashMap<>(); // key 被 value 集合依赖

        // 初始化映射
        for (String nodeName : nodeDefinitionsByName.keySet()) {
            dependsOn.put(nodeName, new HashSet<>());
            dependedBy.put(nodeName, new HashSet<>());
        }

        // 填充依赖关系
        for (DagNodeDefinition<C, ?> nodeDef : nodeDefinitionsByName.values()) {
            String nodeName = nodeDef.getNodeName();
            List<String> dependencies = nodeDef.getDependencyNames();
            if (!dependencies.isEmpty()) {
                for (String depName : dependencies) {
                    dependsOn.computeIfAbsent(nodeName, k -> new HashSet<>()).add(depName);
                    dependedBy.computeIfAbsent(depName, k -> new HashSet<>()).add(nodeName);
                }
            }
        }

        // 按执行顺序列出节点
        List<String> nodesToIterate = executionOrder.isEmpty()
                ? new ArrayList<>(nodeDefinitionsByName.keySet())
                : executionOrder;
        if (executionOrder.isEmpty() && !nodeDefinitionsByName.isEmpty()) {
            Collections.sort(nodesToIterate); // 如果没有执行顺序，按名称排序打印
        }


        if (nodesToIterate.isEmpty()) {
            builder.append("  (DAG 为空)\n");
        } else {
            for (String nodeName : nodesToIterate) {
                DagNodeDefinition<C, ?> nodeDef = nodeDefinitionsByName.get(nodeName);
                if (nodeDef == null) continue; // 防御性检查

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
        dotGraph.append("  rankdir=LR;\n"); // Left to Right layout
        dotGraph.append("  node [shape=record, style=\"rounded,filled\", fillcolor=\"lightblue\", fontname=\"Arial\", fontsize=10];\n");
        dotGraph.append("  edge [fontname=\"Arial\", fontsize=9];\n");

        // 定义节点
        for (DagNodeDefinition<C, ?> nodeDef : nodeDefinitionsByName.values()) {
            String nodeName = nodeDef.getNodeName();
            String logicId = nodeDef.getNodeLogic().getLogicIdentifier();
            String eventType = nodeDef.getEventType().getSimpleName();
            // 节点标签格式: { 节点名 | 逻辑实现 | 事件类型 }
            String label = String.format("{%s | Logic: %s | Event: %s}",
                    nodeName.replace("\"", "\\\""), // 转义引号
                    logicId.replace("\"", "\\\""),
                    eventType.replace("\"", "\\\""));
            dotGraph.append(String.format("  \"%s\" [label=\"%s\"];\n", nodeName, label));
        }

        // 定义边 (依赖关系)
        for (DagNodeDefinition<C, ?> nodeDef : nodeDefinitionsByName.values()) {
            String dependerName = nodeDef.getNodeName();
            List<String> dependencies = nodeDef.getDependencyNames();
            if (!dependencies.isEmpty()) {
                for (String dependencyName : dependencies) {
                    // 确保依赖节点也存在 (理论上验证过)
                    if (nodeDefinitionsByName.containsKey(dependencyName)) {
                        // 边: dependencyName -> dependerName
                        dotGraph.append(String.format("  \"%s\" -> \"%s\";\n", dependencyName, dependerName));
                    }
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
