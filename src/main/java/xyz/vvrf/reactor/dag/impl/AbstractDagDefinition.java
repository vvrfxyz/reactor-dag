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
 * </p>
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen
 */
@Slf4j
public abstract class AbstractDagDefinition<C> implements DagDefinition<C> {

    // Key: Node Name, Value: Map<Payload Type Class, Node Instance>
    // 考虑到节点名称唯一，内层 Map 理论上只有一个元素，但保留结构以支持按类型查找
    private final Map<String, Map<Class<?>, DagNode<C, ?, ?>>> nodesByType = new ConcurrentHashMap<>();
    // Key: Node Name, Value: Node Instance (Payload/Event types are wildcarded)
    // 由于节点名称唯一，此 Map 是节点的主要存储
    private final Map<String, DagNode<C, ?, ?>> nodesByName = new ConcurrentHashMap<>();

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
            this.initialized = true; // 标记为空但已初始化（无事可做）
        } else {
            // 在构造函数中直接注册，如果失败则构造失败
            try {
                registerNodes(nodes);
            } catch (IllegalStateException e) {
                log.error("[{}] DAG '{}' 节点注册失败: {}", contextType.getSimpleName(), getDagName(), e.getMessage());
                // 清理可能已部分注册的节点，确保状态一致性
                nodesByName.clear();
                nodesByType.clear();
                this.initialized = false; // 注册失败，未初始化
                throw e; // 将异常抛出，构造函数失败
            }
            // 初始化通常由外部调用 initializeIfNeeded() 或 initialize() 触发
        }
    }

    /**
     * 如果需要，初始化DAG定义
     * 该方法可以被子类或容器（如Spring）在适当的时机调用
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

    @Override
    public synchronized void initialize() throws IllegalStateException {
        if (initialized) {
            // 如果是因为构造时 nodes 为空而标记为 initialized，这里允许重新初始化（如果节点后续被添加）
            // 但当前设计不允许后续添加节点，所以这个检查是有效的
            log.debug("[{}] DAG '{}' 已初始化，跳过", contextType.getSimpleName(), getDagName());
            return;
        }
        // 检查构造函数中是否因注册失败而未设置 initialized
        if (nodesByName.isEmpty() && !nodesByType.isEmpty()) {
            // 理论上不应发生，因为 registerNodes 失败会清理 maps
            log.error("[{}] DAG '{}' 状态不一致：nodesByName 为空但 nodesByType 不为空。初始化中止。", contextType.getSimpleName(), getDagName());
            throw new IllegalStateException("DAG state inconsistent during initialization.");
        }

        if (nodesByName.isEmpty()) {
            log.warn("[{}] DAG '{}' 为空，跳过验证和排序", contextType.getSimpleName(), getDagName());
            this.executionOrder = Collections.emptyList();
            this.initialized = true; // 标记为空但已初始化
            return;
        }

        log.info("[{}] 开始初始化和验证 DAG '{}'...", contextType.getSimpleName(), getDagName());
        try {
            validateDependencies();
            detectCycles();
            this.executionOrder = calculateExecutionOrder();
            this.initialized = true;

            log.info("[{}] DAG '{}' 验证成功", contextType.getSimpleName(), getDagName());
            log.info("[{}] DAG '{}' 执行顺序: {}", contextType.getSimpleName(), getDagName(), executionOrder);
            printDagStructure();
            generateDotGraph();
        } catch (IllegalStateException e) {
            log.error("[{}] DAG '{}' 验证失败: {}", contextType.getSimpleName(), getDagName(), e.getMessage());
            this.executionOrder = Collections.emptyList();
            this.initialized = false; // 保持未初始化状态
            throw e; // 将验证异常抛出
        }
    }

    /**
     * 注册节点列表。
     * <p>
     * 强制要求节点名称必须唯一。如果检测到重复名称，将抛出 {@link IllegalStateException}。
     * </p>
     *
     * @param nodes 要注册的节点列表
     * @throws IllegalStateException 如果节点名称为空、Payload 类型为 null 或节点名称重复
     */
    private void registerNodes(List<DagNode<C, ?, ?>> nodes) throws IllegalStateException {
        log.debug("[{}] DAG '{}': 开始注册 {} 个提供的节点...", contextType.getSimpleName(), getDagName(), nodes.size());
        for (DagNode<C, ?, ?> node : nodes) {
            String nodeName = node.getName();
            // 使用 getPayloadType() 获取节点的 Payload 类型
            Class<?> payloadType = node.getPayloadType();

            // 1. 验证节点名称
            if (nodeName == null || nodeName.trim().isEmpty()) {
                String errorMsg = String.format("[%s] DAG '%s': 检测到未命名节点 (实现类: %s)。节点必须有名称。",
                        contextType.getSimpleName(), getDagName(), node.getClass().getName());
                log.error(errorMsg);
                throw new IllegalStateException(errorMsg); // 严格模式：不允许未命名节点
                // log.error("[{}] DAG '{}': 跳过未命名节点: {}", contextType.getSimpleName(), getDagName(), node.getClass().getName());
                // continue; // 或者选择跳过
            }

            // 2. 验证 Payload 类型
            if (payloadType == null) {
                // getPayloadType() 不应返回 null
                String errorMsg = String.format("[%s] DAG '%s': 节点 '%s' (实现类: %s) 未提供有效的 Payload 类型 (getPayloadType() 返回 null)。",
                        contextType.getSimpleName(), getDagName(), nodeName, node.getClass().getName());
                log.error(errorMsg);
                throw new IllegalStateException(errorMsg); // 严格模式：Payload 类型不能为空
            }

            // 3. 检查名称唯一性 (Intent A Enforcement)
            if (nodesByName.containsKey(nodeName)) {
                DagNode<C, ?, ?> existingNode = nodesByName.get(nodeName);
                String errorMsg = String.format(
                        "[%s] DAG '%s': 节点名称冲突！名称 '%s' 已被节点 '%s' 使用，不能再被节点 '%s' 使用。节点名称在 DAG 中必须唯一。",
                        contextType.getSimpleName(), getDagName(), nodeName,
                        existingNode.getClass().getName(), // 已存在的节点实现类
                        node.getClass().getName()          // 尝试注册的节点实现类
                );
                log.error(errorMsg);
                throw new IllegalStateException(errorMsg); // 发现重复名称，注册失败
            }

            // 4. 名称唯一，进行注册
            nodesByName.put(nodeName, node);

            // 同时注册到按类型索引的 Map 中
            // 由于名称已保证唯一，computeIfAbsent 会为新名称创建 Map
            Map<Class<?>, DagNode<C, ?, ?>> typeMap = nodesByType.computeIfAbsent(nodeName, k -> new ConcurrentHashMap<>());
            // 在该名称下放入 Payload 类型 -> 节点的映射
            // 理论上，由于名称唯一，这个 typeMap 只会包含一个 payloadType，但保留结构
            typeMap.put(payloadType, node);

            log.info("[{}] DAG '{}': 已注册节点: '{}' (Payload 类型: {}, 实现: {})",
                    contextType.getSimpleName(), getDagName(), nodeName, payloadType.getSimpleName(), node.getClass().getSimpleName());
        }
        // 移到方法末尾，确保只有成功完成循环后才打印
        log.info("[{}] DAG '{}' 节点注册完成，共 {} 个唯一名称的节点", contextType.getSimpleName(), getDagName(), nodesByName.size());
    }


    // --- 以下方法保持不变，因为它们依赖于 nodesByName 和 nodesByType 的结构 ---
    // --- 并且验证逻辑 (validateDependencies, detectCycles) 已经基于 nodesByName ---
    // --- 拓扑排序 (calculateExecutionOrder) 也基于 nodesByName ---

    private void validateDependencies() {
        log.debug("[{}] DAG '{}': 验证依赖关系...", contextType.getSimpleName(), getDagName());
        for (DagNode<C, ?, ?> node : getAllNodes()) { // getAllNodes() 返回 nodesByName.values()
            List<DependencyDescriptor> dependencies = node.getDependencies();
            if (dependencies == null || dependencies.isEmpty()) continue;

            for (DependencyDescriptor dep : dependencies) {
                // 检查依赖的节点是否存在 (基于名称)
                if (!nodesByName.containsKey(dep.getName())) {
                    String errorMsg = String.format("节点 '%s' (%s) 依赖不存在的节点 '%s'",
                            node.getName(), node.getClass().getSimpleName(), dep.getName());
                    throw new IllegalStateException(String.format("[%s] DAG '%s': %s", contextType.getSimpleName(), getDagName(), errorMsg));
                }
                // 检查依赖的节点是否支持所需的 Payload 类型
                if (!supportsOutputType(dep.getName(), dep.getRequiredType())) {
                    String errorMsg = formatDependencyError(node, dep);
                    throw new IllegalStateException(String.format("[%s] DAG '%s': %s", contextType.getSimpleName(), getDagName(), errorMsg));
                }
            }
        }
        log.info("[{}] DAG '{}': 依赖关系验证通过", contextType.getSimpleName(), getDagName());
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

    private void detectCycles() {
        log.debug("[{}] DAG '{}': 检测循环依赖...", contextType.getSimpleName(), getDagName());
        Set<String> visited = new HashSet<>(); // 完全访问过的节点
        Set<String> visiting = new HashSet<>(); // 当前 DFS 路径上的节点
        for (String nodeName : nodesByName.keySet()) {
            if (!visited.contains(nodeName)) {
                checkCycleDFS(nodeName, visiting, visited);
            }
        }
        log.info("[{}] DAG '{}': 未检测到循环依赖", contextType.getSimpleName(), getDagName());
    }

    private void checkCycleDFS(String nodeName, Set<String> visiting, Set<String> visited) {
        visiting.add(nodeName); // 标记为正在访问

        DagNode<C, ?, ?> node = nodesByName.get(nodeName);
        if (node == null) { // 防御性检查
            log.warn("[{}] DAG '{}': 在循环检测中遇到未注册的节点 '{}' (这不应该发生)", contextType.getSimpleName(), getDagName(), nodeName);
            visiting.remove(nodeName);
            visited.add(nodeName);
            return;
        }

        List<DependencyDescriptor> dependencies = node.getDependencies();
        if (dependencies != null) {
            for (DependencyDescriptor dep : dependencies) {
                String depName = dep.getName();

                // 验证阶段已检查依赖是否存在，这里再次确认
                if (!nodesByName.containsKey(depName)) {
                    throw new IllegalStateException(String.format("[%s] DAG '%s': 节点 '%s' 依赖不存在的节点 '%s' (在循环检测中发现)",
                            contextType.getSimpleName(), getDagName(), nodeName, depName));
                }

                if (visiting.contains(depName)) {
                    throw new IllegalStateException(
                            String.format("[%s] DAG '%s': 检测到循环依赖！路径涉及 '%s' -> '%s'",
                                    contextType.getSimpleName(), getDagName(), depName, nodeName));
                }

                if (!visited.contains(depName)) {
                    checkCycleDFS(depName, visiting, visited);
                }
            }
        }

        visiting.remove(nodeName); // 移出当前路径
        visited.add(nodeName);     // 标记为完全访问过
    }


    private List<String> calculateExecutionOrder() {
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> adj = new HashMap<>(); // 存储依赖关系： key -> list of nodes that depend on key

        // 初始化入度和邻接表 (基于唯一的节点名称)
        for (String nodeName : nodesByName.keySet()) {
            inDegree.put(nodeName, 0);
            adj.put(nodeName, new ArrayList<>());
        }

        // 构建图和计算入度
        for (DagNode<C, ?, ?> node : nodesByName.values()) {
            String dependerName = node.getName(); // 当前节点
            List<DependencyDescriptor> dependencies = node.getDependencies();
            if (dependencies != null) {
                for (DependencyDescriptor dep : dependencies) {
                    String dependencyName = dep.getName(); // 当前节点依赖的节点

                    // 确保依赖节点存在 (validateDependencies 已保证)
                    if (adj.containsKey(dependencyName)) {
                        adj.get(dependencyName).add(dependerName);
                        inDegree.put(dependerName, inDegree.get(dependerName) + 1);
                    } else {
                        throw new IllegalStateException(String.format("[%s] DAG '%s': 在拓扑排序中发现未经验证的依赖 '%s' -> '%s'",
                                contextType.getSimpleName(), getDagName(), dependencyName, dependerName));
                    }
                }
            }
        }

        // Kahn's Algorithm
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
        if (!log.isInfoEnabled()) return;

        log.info("[{}] DAG '{}' 结构表示:", contextType.getSimpleName(), getDagName());
        StringBuilder builder = new StringBuilder("\n");
        // 调整列宽
        builder.append(String.format("%-40s | %-30s | %-40s | %-40s\n",
                "节点名称 (Payload 类型)", "实现类", "依赖节点 (所需 Payload)", "被依赖节点"));
        StringBuilder divider = new StringBuilder();
        for (int i = 0; i < 155; i++) divider.append("-");
        builder.append(divider).append("\n");


        Map<String, Set<String>> dependsOn = new HashMap<>();
        Map<String, Set<String>> dependedBy = new HashMap<>();
        for (String nodeName : nodesByName.keySet()) {
            dependsOn.put(nodeName, new HashSet<>());
            dependedBy.put(nodeName, new HashSet<>());
        }
        for (DagNode<C, ?, ?> node : getAllNodes()) {
            String nodeName = node.getName();
            List<DependencyDescriptor> dependencies = node.getDependencies();
            if (dependencies != null) {
                for (DependencyDescriptor dep : dependencies) {
                    String depName = dep.getName();
                    // 依赖信息包含所需 Payload 类型
                    String depInfo = String.format("%s (%s)", depName, dep.getRequiredType().getSimpleName());
                    dependsOn.computeIfAbsent(nodeName, k -> new HashSet<>()).add(depInfo);
                    dependedBy.computeIfAbsent(depName, k -> new HashSet<>()).add(nodeName);
                }
            }
        }

        for (String nodeName : executionOrder) {
            DagNode<C, ?, ?> node = nodesByName.get(nodeName);
            if (node == null) continue;

            String nameAndPayloadType = String.format("%s (%s)", nodeName, node.getPayloadType().getSimpleName());
            String implClass = node.getClass().getSimpleName();
            String dependsOnStr = formatNodeSet(dependsOn.get(nodeName));
            String dependedByStr = formatNodeSet(dependedBy.get(nodeName));

            builder.append(String.format("%-40s | %-30s | %-40s | %-40s\n",
                    nameAndPayloadType, implClass, dependsOnStr, dependedByStr));
        }

        builder.append("\n执行路径 (→ 表示执行顺序):\n");
        builder.append(String.join(" → ", executionOrder));
        log.info(builder.toString());
    }

    private void generateDotGraph() {
        // ... (DOT graph generation logic remains the same, based on nodesByName and dependencies) ...
        if (!log.isInfoEnabled()) return;

        StringBuilder dotGraph = new StringBuilder();
        String graphName = String.format("DAG_%s_%s", contextType.getSimpleName(), getDagName().replaceAll("\\W+", "_"));
        dotGraph.append(String.format("digraph %s {\n", graphName));
        dotGraph.append(String.format("  label=\"DAG Structure (%s - %s)\";\n", contextType.getSimpleName(), getDagName()));
        dotGraph.append("  labelloc=top;\n");
        dotGraph.append("  fontsize=16;\n");
        dotGraph.append("  rankdir=LR;\n");
        dotGraph.append("  node [shape=record, style=\"rounded,filled\", fillcolor=\"lightblue\", fontname=\"Arial\", fontsize=10];\n");
        dotGraph.append("  edge [fontname=\"Arial\", fontsize=9];\n");

        for (String nodeName : nodesByName.keySet()) {
            DagNode<C, ?, ?> node = nodesByName.get(nodeName);
            if (node == null) continue;
            String label = String.format("{%s | Payload: %s | Impl: %s}",
                    nodeName,
                    node.getPayloadType().getSimpleName(),
                    node.getClass().getSimpleName());
            dotGraph.append(String.format("  \"%s\" [label=\"%s\"];\n", nodeName, label));
        }

        for (DagNode<C, ?, ?> node : getAllNodes()) {
            String nodeName = node.getName();
            List<DependencyDescriptor> dependencies = node.getDependencies();
            if (dependencies != null) {
                for (DependencyDescriptor dep : dependencies) {
                    String depName = dep.getName();
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


    private String formatNodeSet(Set<String> nodeSet) {
        if (nodeSet == null || nodeSet.isEmpty()) return "无";
        List<String> sortedNodes = new ArrayList<>(nodeSet);
        Collections.sort(sortedNodes);
        return String.join(", ", sortedNodes);
    }

    // --- DagDefinition 接口实现 ---

    @Override
    @SuppressWarnings("unchecked")
    public <P> Optional<DagNode<C, P, ?>> getNode(String nodeName, Class<P> payloadType) {
        // 由于名称唯一，先按名称查找
        DagNode<C, ?, ?> node = nodesByName.get(nodeName);
        if (node == null) {
            return Optional.empty(); // 节点名称不存在
        }
        // 检查找到的唯一节点的 Payload 类型是否与请求的类型匹配或兼容
        if (payloadType.isAssignableFrom(node.getPayloadType())) {
            // 类型匹配，安全转换
            return Optional.of((DagNode<C, P, ?>) node);
        } else {
            // 名称存在，但 Payload 类型不匹配
            log.warn("[{}] DAG '{}': 请求节点 '{}' 的 Payload 类型 '{}'，但该节点实际 Payload 类型为 '{}'",
                    contextType.getSimpleName(), getDagName(), nodeName,
                    payloadType.getSimpleName(), node.getPayloadType().getSimpleName());
            return Optional.empty();
        }
        // 原 nodesByType 的查找逻辑不再需要，因为名称唯一确定了节点
    }

    @Override
    public Optional<DagNode<C, ?, ?>> getNodeAnyType(String nodeName) {
        // 直接从按名称索引的 Map 中获取，因为名称是唯一的
        return Optional.ofNullable(nodesByName.get(nodeName));
    }

    @Override
    public Collection<DagNode<C, ?, ?>> getAllNodes() {
        // 返回按名称索引的 Map 的值集合 (保证了唯一性)
        return Collections.unmodifiableCollection(nodesByName.values());
    }

    @Override
    public Set<Class<?>> getSupportedOutputTypes(String nodeName) {
        // 由于名称唯一，一个节点只有一个 Payload 输出类型
        return getNodeAnyType(nodeName)
                .map(node -> Collections.<Class<?>>singleton(node.getPayloadType())) // 返回包含其唯一 Payload 类型的 Set
                .orElse(Collections.emptySet()); // 如果节点不存在，返回空 Set
    }

    @Override
    public <P> boolean supportsOutputType(String nodeName, Class<P> payloadType) {
        // 检查节点是否存在，并且其唯一的 Payload 类型是否与请求的类型匹配或兼容
        return getNodeAnyType(nodeName)
                .map(node -> payloadType.isAssignableFrom(node.getPayloadType()))
                .orElse(false);
    }

    /**
     * 检查此 DAG 定义是否已成功初始化（包括验证和拓扑排序）。
     *
     * @return 如果已初始化则返回 true，否则返回 false。
     */
    public boolean isInitialized() {
        return initialized;
    }
}
