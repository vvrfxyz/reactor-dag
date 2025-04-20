// abstractdagdefinition.java
package xyz.vvrf.reactor.dag.impl;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.DependencyDescriptor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * DagDefinition 的抽象基类，提供通用的节点管理、验证和拓扑排序逻辑。
 * <p>
 * 此实现强制要求 DAG 内的节点名称必须唯一。
 * 支持通过 addExplicitDependencies 方法覆盖节点自身的依赖定义。
 * </p>
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen (modified with ChainBuilder support)
 */
@Slf4j
public abstract class AbstractDagDefinition<C> implements DagDefinition<C> {

    // ... (nodesByType, nodesByName 保持不变)
    private final Map<String, DagNode<C, ?, ?>> nodesByName = new ConcurrentHashMap<>();
    private final Map<String, Map<Class<?>, DagNode<C, ?, ?>>> nodesByType = new ConcurrentHashMap<>(); // 保留用于可能的类型查找，但主要依赖 nodesByName

    // 新增：存储由外部（如 ChainBuilder）定义的显式依赖
    // Key: Node Name, Value: List of Dependency Descriptors for this node
    private final Map<String, List<DependencyDescriptor>> explicitDependencies = new ConcurrentHashMap<>();

    @Getter
    private List<String> executionOrder = Collections.emptyList();

    private final Class<C> contextType;
    private boolean initialized = false;

    /**
     * 构造函数
     *
     * @param contextType 此 DAG 定义关联的上下文类型
     * @param nodes 实现此 DAG 的节点列表 (通常通过依赖注入传入)
     * @throws IllegalStateException 如果在注册期间发现重复的节点名称
     */
    protected AbstractDagDefinition(Class<C> contextType, List<DagNode<C, ?, ?>> nodes) {
        this.contextType = Objects.requireNonNull(contextType, "上下文类型不能为空");
        log.info("为上下文 {} 初始化 DAG 定义 '{}'...", contextType.getSimpleName(), getDagName());
        if (nodes == null || nodes.isEmpty()) {
            log.warn("[{}] 未找到任何 DagNode<C, ?, ?> 类型的节点，DAG '{}' 将不可用", contextType.getSimpleName(), getDagName());
            this.executionOrder = Collections.emptyList();
            // 注意：这里不应标记为 initialized=true，因为子类可能稍后添加显式依赖并调用 initialize
            this.initialized = false;
        } else {
            try {
                registerNodes(nodes);
                // 不在此处调用 initialize()，允许子类在构造函数中设置显式依赖后再初始化
            } catch (IllegalStateException e) {
                log.error("[{}] DAG '{}' 节点注册失败: {}", contextType.getSimpleName(), getDagName(), e.getMessage());
                nodesByName.clear();
                nodesByType.clear();
                explicitDependencies.clear(); // 清理显式依赖
                this.initialized = false;
                throw e;
            }
        }
    }

    /**
     * 如果需要，初始化DAG定义。
     * 该方法可以被子类或容器（如Spring）在适当的时机调用。
     * 必须在所有显式依赖设置完成后调用。
     */
    public synchronized void initializeIfNeeded() {
        if (!initialized) {
            initialize();
        }
    }

    @Override
    public Class<C> getContextType() {
        return contextType;
    }

    /**
     * 为指定节点添加由外部（如 Chain Builder）定义的显式依赖关系。
     * 这将覆盖该节点自身的 getDependencies() 返回值，在图构建时使用。
     * 应在 registerNodes 之后、initialize 之前调用。
     *
     * @param nodeName           需要设置显式依赖的节点名称
     * @param dependencies       该节点的显式依赖列表 (不能为空, 可以是空列表)
     */
    public void addExplicitDependencies(String nodeName, List<DependencyDescriptor> dependencies) {
        Objects.requireNonNull(nodeName, "节点名称不能为空");
        Objects.requireNonNull(dependencies, "显式依赖列表不能为空 (可以是空列表) for node " + nodeName);

        if (!nodesByName.containsKey(nodeName)) {
            log.warn("[{}] DAG '{}': 尝试为未注册的节点 '{}' 添加显式依赖，将被忽略。",
                    contextType.getSimpleName(), getDagName(), nodeName);
            return;
        }
        log.info("[{}] DAG '{}': 为节点 '{}' 设置 {} 条显式依赖 (将覆盖节点自身定义): {}",
                contextType.getSimpleName(), getDagName(), nodeName, dependencies.size(), dependencies);
        this.explicitDependencies.put(nodeName, new ArrayList<>(dependencies)); // 存储副本
    }

    /**
     * 获取节点的有效依赖关系。
     * 优先使用通过 addExplicitDependencies 设置的显式依赖。
     * 如果没有显式依赖，则回退到节点自身的 getDependencies() 定义。
     *
     * @param nodeName 节点名称
     * @return 该节点的有效依赖描述符列表，如果节点不存在则返回空列表。
     */
    @Override
    public List<DependencyDescriptor> getEffectiveDependencies(String nodeName) {
        // 1. 检查显式依赖
        List<DependencyDescriptor> explicitDeps = explicitDependencies.get(nodeName);
        if (explicitDeps != null) {
            // 找到了显式依赖（可能是空列表），直接返回它
            return Collections.unmodifiableList(explicitDeps);
        }

        // 2. 没有显式依赖，回退到节点自身定义
        DagNode<C, ?, ?> node = nodesByName.get(nodeName);
        if (node != null) {
            List<DependencyDescriptor> nodeDeps = node.getDependencies();
            // 返回不可变列表，如果节点返回 null 则返回空列表
            return (nodeDeps != null) ? Collections.unmodifiableList(nodeDeps) : Collections.emptyList();
        } else {
            // 节点不存在
            log.warn("[{}] DAG '{}': 在获取有效依赖时未找到节点 '{}'", contextType.getSimpleName(), getDagName(), nodeName);
            return Collections.emptyList();
        }
    }


    @Override
    public synchronized void initialize() throws IllegalStateException {
        if (initialized) {
            log.debug("[{}] DAG '{}' 已初始化，跳过", contextType.getSimpleName(), getDagName());
            return;
        }
        // 检查注册是否成功
        if (nodesByName.isEmpty() && !explicitDependencies.isEmpty()) {
            log.warn("[{}] DAG '{}' 没有注册节点，但存在显式依赖定义，这通常是无效的。继续初始化为空DAG。", contextType.getSimpleName(), getDagName());
        }
        if (nodesByName.isEmpty() && explicitDependencies.isEmpty()) {
            log.warn("[{}] DAG '{}' 为空 (无节点和显式依赖)，跳过验证和排序", contextType.getSimpleName(), getDagName());
            this.executionOrder = Collections.emptyList();
            this.initialized = true; // 标记为空但已初始化
            return;
        }


        log.info("[{}] 开始初始化和验证 DAG '{}' (使用有效依赖)...", contextType.getSimpleName(), getDagName());
        try {
            validateDependencies(); // 使用 getEffectiveDependencies
            detectCycles();         // 使用 getEffectiveDependencies
            this.executionOrder = calculateExecutionOrder(); // 使用 getEffectiveDependencies
            this.initialized = true;

            log.info("[{}] DAG '{}' 验证成功", contextType.getSimpleName(), getDagName());
            log.info("[{}] DAG '{}' 执行顺序: {}", contextType.getSimpleName(), getDagName(), executionOrder);
            printDagStructure();    // 使用 getEffectiveDependencies
            generateDotGraph();     // 使用 getEffectiveDependencies
        } catch (IllegalStateException e) {
            log.error("[{}] DAG '{}' 验证失败: {}", contextType.getSimpleName(), getDagName(), e.getMessage());
            this.executionOrder = Collections.emptyList();
            this.initialized = false; // 保持未初始化状态
            throw e; // 将验证异常抛出
        }
    }

    // --- 修改以下方法以使用 getEffectiveDependencies ---

    private void validateDependencies() {
        log.debug("[{}] DAG '{}': 验证依赖关系 (使用有效依赖)...", contextType.getSimpleName(), getDagName());
        for (DagNode<C, ?, ?> node : getAllNodes()) {
            String nodeName = node.getName();
            // *** 使用 getEffectiveDependencies ***
            List<DependencyDescriptor> dependencies = getEffectiveDependencies(nodeName);
            if (dependencies.isEmpty()) continue;

            for (DependencyDescriptor dep : dependencies) {
                // 检查依赖的节点是否存在 (基于名称)
                if (!nodesByName.containsKey(dep.getName())) {
                    String errorMsg = String.format("节点 '%s' (%s) 依赖不存在的节点 '%s'",
                            nodeName, node.getClass().getSimpleName(), dep.getName());
                    throw new IllegalStateException(String.format("[%s] DAG '%s': %s", contextType.getSimpleName(), getDagName(), errorMsg));
                }
                // 检查依赖的节点是否支持所需的 Payload 类型
                if (!supportsOutputType(dep.getName(), dep.getRequiredType())) {
                    // formatDependencyError 内部也需要基于 nodesByName 获取依赖节点信息，保持不变
                    String errorMsg = formatDependencyError(node, dep);
                    throw new IllegalStateException(String.format("[%s] DAG '%s': %s", contextType.getSimpleName(), getDagName(), errorMsg));
                }
            }
        }
        log.info("[{}] DAG '{}': 依赖关系验证通过", contextType.getSimpleName(), getDagName());
    }

    private void detectCycles() {
        log.debug("[{}] DAG '{}': 检测循环依赖 (使用有效依赖)...", contextType.getSimpleName(), getDagName());
        Set<String> visited = new HashSet<>();
        Set<String> visiting = new HashSet<>();
        for (String nodeName : nodesByName.keySet()) {
            if (!visited.contains(nodeName)) {
                checkCycleDFS(nodeName, visiting, visited); // checkCycleDFS 内部会获取依赖
            }
        }
        log.info("[{}] DAG '{}': 未检测到循环依赖", contextType.getSimpleName(), getDagName());
    }

    private void checkCycleDFS(String nodeName, Set<String> visiting, Set<String> visited) {
        visiting.add(nodeName);

        // *** 使用 getEffectiveDependencies ***
        List<DependencyDescriptor> dependencies = getEffectiveDependencies(nodeName);

        if (!dependencies.isEmpty()) {
            for (DependencyDescriptor dep : dependencies) {
                String depName = dep.getName();

                // 验证阶段已检查依赖是否存在，这里再次确认
                if (!nodesByName.containsKey(depName)) {
                    // 这个错误理论上不应发生，因为 validateDependencies 先运行
                    throw new IllegalStateException(String.format("[%s] DAG '%s': 节点 '%s' 依赖不存在的节点 '%s' (在循环检测中发现)",
                            contextType.getSimpleName(), getDagName(), nodeName, depName));
                }

                if (visiting.contains(depName)) {
                    // 构建更详细的循环路径可能需要回溯，这里简单报告涉及的边
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
        log.debug("[{}] DAG '{}': 计算拓扑执行顺序 (使用有效依赖)...", contextType.getSimpleName(), getDagName());
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> adj = new HashMap<>(); // 存储依赖关系： key -> list of nodes that depend on key

        // 初始化入度和邻接表
        for (String nodeName : nodesByName.keySet()) {
            inDegree.put(nodeName, 0);
            adj.put(nodeName, new ArrayList<>());
        }

        // 构建图和计算入度
        for (DagNode<C, ?, ?> node : nodesByName.values()) {
            String dependerName = node.getName(); // 当前节点
            // *** 使用 getEffectiveDependencies ***
            List<DependencyDescriptor> dependencies = getEffectiveDependencies(dependerName);

            if (!dependencies.isEmpty()) {
                for (DependencyDescriptor dep : dependencies) {
                    String dependencyName = dep.getName(); // 当前节点依赖的节点

                    // 确保依赖节点存在 (validateDependencies 已保证)
                    if (adj.containsKey(dependencyName)) {
                        // 添加边：dependencyName -> dependerName
                        adj.get(dependencyName).add(dependerName);
                        // 增加 dependerName 的入度
                        inDegree.put(dependerName, inDegree.get(dependerName) + 1);
                    } else {
                        // 这个错误理论上不应发生
                        throw new IllegalStateException(String.format("[%s] DAG '%s': 在拓扑排序中发现未经验证的依赖 '%s' -> '%s'",
                                contextType.getSimpleName(), getDagName(), dependencyName, dependerName));
                    }
                }
            }
        }

        // Kahn's Algorithm (保持不变)
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

        if (sortedOrder.size() != nodesByName.size()) {
            Set<String> remainingNodes = nodesByName.keySet().stream()
                    .filter(n -> !sortedOrder.contains(n))
                    .collect(Collectors.toSet());
            log.error("[{}] DAG '{}': 拓扑排序失败，图中存在循环。未排序节点（可能参与循环）: {}",
                    contextType.getSimpleName(), getDagName(), remainingNodes);
            throw new IllegalStateException(String.format("[%s] DAG '%s': 拓扑排序失败，检测到循环。未排序节点: %s",
                    contextType.getSimpleName(), getDagName(), remainingNodes));
        }

        return Collections.unmodifiableList(sortedOrder);
    }

    private void printDagStructure() {
        if (!log.isInfoEnabled() || nodesByName.isEmpty()) return; // 如果没有节点，不打印

        log.info("[{}] DAG '{}' 结构表示 (使用有效依赖):", contextType.getSimpleName(), getDagName());
        StringBuilder builder = new StringBuilder("\n");
        builder.append(String.format("%-40s | %-30s | %-40s | %-40s\n",
                "节点名称 (Payload 类型)", "实现类", "依赖节点 (所需 Payload)", "被依赖节点"));
        StringBuilder divider = new StringBuilder();
        for (int i = 0; i < 155; i++) divider.append("-");
        builder.append(divider).append("\n");

        Map<String, Set<String>> dependsOn = new HashMap<>(); // key 依赖于 value set
        Map<String, Set<String>> dependedBy = new HashMap<>(); // key 被 value set 依赖

        for (String nodeName : nodesByName.keySet()) {
            dependsOn.put(nodeName, new HashSet<>());
            dependedBy.put(nodeName, new HashSet<>());
        }

        for (DagNode<C, ?, ?> node : getAllNodes()) {
            String nodeName = node.getName();
            // *** 使用 getEffectiveDependencies ***
            List<DependencyDescriptor> dependencies = getEffectiveDependencies(nodeName);
            if (!dependencies.isEmpty()) {
                for (DependencyDescriptor dep : dependencies) {
                    String depName = dep.getName();
                    // 依赖信息包含所需 Payload 类型
                    String depInfo = String.format("%s (%s)", depName, dep.getRequiredType().getSimpleName());
                    dependsOn.computeIfAbsent(nodeName, k -> new HashSet<>()).add(depInfo);
                    dependedBy.computeIfAbsent(depName, k -> new HashSet<>()).add(nodeName);
                }
            }
        }

        // 确保按执行顺序列出
        List<String> nodesToIterate; // 用于迭代打印的节点列表
        if (!executionOrder.isEmpty()) {
            // 如果有执行顺序，直接使用它，不需要排序
            nodesToIterate = executionOrder;
            log.debug("Printing DAG structure using execution order.");
        } else if (!nodesByName.isEmpty()) {
            // 如果没有执行顺序，但有节点，则按名称排序打印
            log.debug("Printing DAG structure sorted by node name (no execution order available).");
            nodesToIterate = new ArrayList<>(nodesByName.keySet());
            Collections.sort(nodesToIterate); // 对新创建的 ArrayList 排序
        } else {
            // 没有节点
            nodesToIterate = Collections.emptyList();
        }

        if (nodesToIterate.isEmpty()) {
            builder.append("  (DAG 为空)\n");
        } else {
            // 使用确定好的列表进行迭代
            for (String nodeName : nodesToIterate) {
                DagNode<C, ?, ?> node = nodesByName.get(nodeName);
                if (node == null) continue; // 防御性检查

                String nameAndPayloadType = String.format("%s (%s)", nodeName, node.getPayloadType().getSimpleName());
                String implClass = node.getClass().getSimpleName();
                String dependsOnStr = formatNodeSet(dependsOn.get(nodeName));
                String dependedByStr = formatNodeSet(dependedBy.get(nodeName));

                builder.append(String.format("%-40s | %-30s | %-40s | %-40s\n",
                        nameAndPayloadType, implClass, dependsOnStr, dependedByStr));
            }
        }


        if (!executionOrder.isEmpty()) {
            builder.append("\n执行路径 (→ 表示执行顺序):\n");
            builder.append(String.join(" → ", executionOrder));
        } else if (!nodesByName.isEmpty()) {
            builder.append("\n(无有效执行路径 - 可能存在循环或未初始化)\n");
        }

        log.info(builder.toString());
    }

    private void generateDotGraph() {
        if (!log.isInfoEnabled() || nodesByName.isEmpty()) return;

        StringBuilder dotGraph = new StringBuilder();
        String graphName = String.format("DAG_%s_%s", contextType.getSimpleName(), getDagName().replaceAll("\\W+", "_"));
        dotGraph.append(String.format("digraph %s {\n", graphName));
        dotGraph.append(String.format("  label=\"DAG Structure (%s - %s) - Effective Dependencies\";\n", contextType.getSimpleName(), getDagName()));
        dotGraph.append("  labelloc=top;\n");
        dotGraph.append("  fontsize=16;\n");
        dotGraph.append("  rankdir=LR;\n"); // Left-to-Right layout
        dotGraph.append("  node [shape=record, style=\"rounded,filled\", fillcolor=\"lightblue\", fontname=\"Arial\", fontsize=10];\n");
        dotGraph.append("  edge [fontname=\"Arial\", fontsize=9];\n");

        // 添加节点
        for (String nodeName : nodesByName.keySet()) {
            DagNode<C, ?, ?> node = nodesByName.get(nodeName);
            if (node == null) continue;
            String label = String.format("{%s | Payload: %s | EventType: %s}",
                    nodeName,
                    node.getPayloadType().getSimpleName(),
                    node.getEventType().getSimpleName());
            dotGraph.append(String.format("  \"%s\" [label=\"%s\"];\n", nodeName, label));
        }

        // 添加边 (基于有效依赖)
        for (DagNode<C, ?, ?> node : getAllNodes()) {
            String nodeName = node.getName();
            // *** 使用 getEffectiveDependencies ***
            List<DependencyDescriptor> dependencies = getEffectiveDependencies(nodeName);
            if (!dependencies.isEmpty()) {
                for (DependencyDescriptor dep : dependencies) {
                    String depName = dep.getName();
                    // 确保两个节点都存在才画边
                    if (nodesByName.containsKey(depName) && nodesByName.containsKey(nodeName)) {
                        String edgeLabel = String.format("Payload: %s", dep.getRequiredType().getSimpleName());
                        dotGraph.append(String.format("  \"%s\" -> \"%s\" [label=\"%s\"];\n", depName, nodeName, edgeLabel));
                    }
                }
            }
        }
        dotGraph.append("}\n");
        log.info("[{}] DAG '{}' DOT格式图 (可使用 Graphviz 查看):\n{}", contextType.getSimpleName(), getDagName(), dotGraph.toString());
    }

    // --- registerNodes, formatNodeSet, getNode, getNodeAnyType, getAllNodes, getSupportedOutputTypes, supportsOutputType, isInitialized 保持不变 ---
    // registerNodes 逻辑不变，它只负责注册节点实例
    private void registerNodes(List<DagNode<C, ?, ?>> nodes) throws IllegalStateException {
        log.debug("[{}] DAG '{}': 开始注册 {} 个提供的节点...", contextType.getSimpleName(), getDagName(), nodes.size());
        for (DagNode<C, ?, ?> node : nodes) {
            String nodeName = node.getName();
            Class<?> payloadType = node.getPayloadType();

            if (nodeName == null || nodeName.trim().isEmpty()) {
                String errorMsg = String.format("[%s] DAG '%s': 检测到未命名节点 (实现类: %s)。节点必须有名称。",
                        contextType.getSimpleName(), getDagName(), node.getClass().getName());
                log.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            if (payloadType == null) {
                String errorMsg = String.format("[%s] DAG '%s': 节点 '%s' (实现类: %s) 未提供有效的 Payload 类型 (getPayloadType() 返回 null)。",
                        contextType.getSimpleName(), getDagName(), nodeName, node.getClass().getName());
                log.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            if (nodesByName.containsKey(nodeName)) {
                DagNode<C, ?, ?> existingNode = nodesByName.get(nodeName);
                String errorMsg = String.format(
                        "[%s] DAG '%s': 节点名称冲突！名称 '%s' 已被节点 '%s' 使用，不能再被节点 '%s' 使用。节点名称在 DAG 中必须唯一。",
                        contextType.getSimpleName(), getDagName(), nodeName,
                        existingNode.getClass().getName(),
                        node.getClass().getName()
                );
                log.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }

            nodesByName.put(nodeName, node);
            Map<Class<?>, DagNode<C, ?, ?>> typeMap = nodesByType.computeIfAbsent(nodeName, k -> new ConcurrentHashMap<>());
            typeMap.put(payloadType, node);

            log.info("[{}] DAG '{}': 已注册节点: '{}' (Payload 类型: {}, 实现: {})",
                    contextType.getSimpleName(), getDagName(), nodeName, payloadType.getSimpleName(), node.getClass().getSimpleName());
        }
        log.info("[{}] DAG '{}' 节点注册完成，共 {} 个唯一名称的节点", contextType.getSimpleName(), getDagName(), nodesByName.size());
    }

    private String formatNodeSet(Set<String> nodeSet) {
        if (nodeSet == null || nodeSet.isEmpty()) return "无";
        List<String> sortedNodes = new ArrayList<>(nodeSet);
        Collections.sort(sortedNodes);
        return String.join(", ", sortedNodes);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> Optional<DagNode<C, P, ?>> getNode(String nodeName, Class<P> payloadType) {
        DagNode<C, ?, ?> node = nodesByName.get(nodeName);
        if (node == null) {
            return Optional.empty();
        }
        if (payloadType.isAssignableFrom(node.getPayloadType())) {
            return Optional.of((DagNode<C, P, ?>) node);
        } else {
            log.warn("[{}] DAG '{}': 请求节点 '{}' 的 Payload 类型 '{}'，但该节点实际 Payload 类型为 '{}'",
                    contextType.getSimpleName(), getDagName(), nodeName,
                    payloadType.getSimpleName(), node.getPayloadType().getSimpleName());
            return Optional.empty();
        }
    }

    private String formatDependencyError(DagNode<C, ?, ?> node, DependencyDescriptor dep) {
        String depName = dep.getName();
        Class<?> requiredPayloadType = dep.getRequiredType();
        // 因为名称唯一，直接从 nodesByName 获取依赖节点实例
        DagNode<C, ?, ?> depNode = nodesByName.get(depName);
        // 之前的检查保证了 depNode 不为 null
        // 检查该唯一实例的 Payload 类型是否匹配
        if (depNode.getPayloadType().equals(requiredPayloadType) || requiredPayloadType.isAssignableFrom(depNode.getPayloadType())) {
            // 类型匹配或兼容，理论上不应进入此 format 方法，但作为防御
            return String.format("内部错误：节点 '%s' 依赖 '%s' 类型 '%s'，但验证逻辑出错。",
                    node.getName(), depName, requiredPayloadType.getSimpleName());
        } else {
            // 依赖节点存在，但其唯一的 Payload 类型不匹配
            return String.format(
                    "节点 '%s' (%s) 依赖节点 '%s' 输出 Payload 类型 '%s'，但节点 '%s' (%s) 实际输出 Payload 类型 '%s'",
                    node.getName(), node.getClass().getSimpleName(),
                    depName, requiredPayloadType.getSimpleName(),
                    depName, depNode.getClass().getSimpleName(), depNode.getPayloadType().getSimpleName()
            );
        }
        // 注意：不再需要 getSupportedOutputTypes，因为每个名称只有一个节点和一个 Payload 类型
    }

    @Override
    public Optional<DagNode<C, ?, ?>> getNodeAnyType(String nodeName) {
        return Optional.ofNullable(nodesByName.get(nodeName));
    }

    @Override
    public Collection<DagNode<C, ?, ?>> getAllNodes() {
        return Collections.unmodifiableCollection(nodesByName.values());
    }

    @Override
    public Set<Class<?>> getSupportedOutputTypes(String nodeName) {
        return getNodeAnyType(nodeName)
                .map(node -> Collections.<Class<?>>singleton(node.getPayloadType()))
                .orElse(Collections.emptySet());
    }

    @Override
    public <P> boolean supportsOutputType(String nodeName, Class<P> payloadType) {
        return getNodeAnyType(nodeName)
                .map(node -> payloadType.isAssignableFrom(node.getPayloadType()))
                .orElse(false);
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }
}
