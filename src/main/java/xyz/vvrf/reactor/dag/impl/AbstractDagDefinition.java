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
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen
 */
@Slf4j
public abstract class AbstractDagDefinition<C> implements DagDefinition<C> {

    private final Map<String, Map<Class<?>, DagNode<C, ?>>> nodesByType = new ConcurrentHashMap<>();
    private final Map<String, DagNode<C, ?>> nodesByName = new ConcurrentHashMap<>();

    @Getter
    private List<String> executionOrder = Collections.emptyList();

    private final Class<C> contextType;
    private boolean initialized = false;

    /**
     * 构造函数
     *
     * @param contextType 此 DAG 定义关联的上下文类型
     * @param nodes 实现此 DAG 的节点列表 (通常通过依赖注入传入)
     */
    protected AbstractDagDefinition(Class<C> contextType, List<DagNode<C, ?>> nodes) {
        this.contextType = Objects.requireNonNull(contextType, "上下文类型不能为空");
        log.info("为上下文 {} 初始化 DAG 定义 '{}'...", contextType.getSimpleName(), getDagName());
        if (nodes == null || nodes.isEmpty()) {
            log.warn("[{}] 未找到任何 DagNode<C, ?> 类型的节点，DAG '{}' 将不可用", contextType.getSimpleName(), getDagName());
            this.executionOrder = Collections.emptyList();
            this.initialized = true;
        } else {
            registerNodes(nodes);
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
            log.debug("[{}] DAG '{}' 已初始化，跳过", contextType.getSimpleName(), getDagName());
            return;
        }
        if (nodesByName.isEmpty()) {
            log.warn("[{}] DAG '{}' 为空，跳过验证和排序", contextType.getSimpleName(), getDagName());
            this.executionOrder = Collections.emptyList();
            this.initialized = true;
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
            this.initialized = false;
            throw e;
        }
    }

    private void registerNodes(List<DagNode<C, ?>> nodes) {
        for (DagNode<C, ?> node : nodes) {
            String nodeName = node.getName();
            Class<?> payloadType = node.getPayloadType();

            if (nodeName == null || nodeName.trim().isEmpty()) {
                log.error("[{}] DAG '{}': 跳过未命名节点: {}", contextType.getSimpleName(), getDagName(), node.getClass().getName());
                continue;
            }
            if (payloadType == null) {
                log.error("[{}] DAG '{}': 节点'{}'未提供有效的输出类型", contextType.getSimpleName(), getDagName(), nodeName);
                continue;
            }

            nodesByName.put(nodeName, node);
            Map<Class<?>, DagNode<C, ?>> typeMap = nodesByType.computeIfAbsent(nodeName, k -> new ConcurrentHashMap<>());

            if (typeMap.containsKey(payloadType)) {
                log.warn("[{}] DAG '{}': 节点 '{}' 对于类型 '{}' 已存在，将被覆盖。实现: {}, 新实现: {}",
                        contextType.getSimpleName(), getDagName(), nodeName, payloadType.getSimpleName(),
                        typeMap.get(payloadType).getClass().getSimpleName(), node.getClass().getSimpleName());
            }
            typeMap.put(payloadType, node);

            log.info("[{}] DAG '{}': 已注册节点: '{}' (输出类型: {}, 实现: {})",
                    contextType.getSimpleName(), getDagName(), nodeName, payloadType.getSimpleName(), node.getClass().getSimpleName());
        }
        log.info("[{}] DAG '{}' 节点注册完成，共 {} 个节点", contextType.getSimpleName(), getDagName(), nodesByName.size());
    }

    private void validateDependencies() {
        log.debug("[{}] DAG '{}': 验证依赖关系...", contextType.getSimpleName(), getDagName());
        for (DagNode<C, ?> node : getAllNodes()) {
            List<DependencyDescriptor> dependencies = node.getDependencies();
            if (dependencies == null || dependencies.isEmpty()) continue;

            for (DependencyDescriptor dep : dependencies) {
                if (!supportsOutputType(dep.getName(), dep.getRequiredType())) {
                    String errorMsg = formatDependencyError(node, dep);
                    throw new IllegalStateException(String.format("[%s] DAG '%s': %s", contextType.getSimpleName(), getDagName(), errorMsg));
                }
            }
        }
        log.info("[{}] DAG '{}': 依赖关系验证通过", contextType.getSimpleName(), getDagName());
    }

    private String formatDependencyError(DagNode<C, ?> node, DependencyDescriptor dep) {
        String depName = dep.getName();
        Class<?> requiredType = dep.getRequiredType();
        Optional<DagNode<C, ?>> anyDepNode = getNodeAnyType(depName);
        if (anyDepNode.isPresent()) {
            Set<Class<?>> availableTypes = getSupportedOutputTypes(depName);
            return String.format(
                    "节点'%s'依赖'%s'(类型:%s)，但该节点只支持类型%s",
                    node.getName(), depName, requiredType.getSimpleName(),
                    availableTypes.stream().map(Class::getSimpleName).collect(Collectors.toList())
            );
        } else {
            return String.format("节点'%s'依赖不存在的节点'%s'", node.getName(), depName);
        }
    }

    private void detectCycles() {
        log.debug("[{}] DAG '{}': 检测循环依赖...", contextType.getSimpleName(), getDagName());
        Set<String> visited = new HashSet<>();
        Set<String> visiting = new HashSet<>();
        for (String nodeName : nodesByName.keySet()) {
            if (!visited.contains(nodeName)) {
                checkCycleDFS(nodeName, visiting, visited);
            }
        }
        log.info("[{}] DAG '{}': 未检测到循环依赖", contextType.getSimpleName(), getDagName());
    }

    private void checkCycleDFS(String nodeName, Set<String> visiting, Set<String> visited) {
        visiting.add(nodeName);
        DagNode<C, ?> node = nodesByName.get(nodeName);
        if (node == null) {
            visiting.remove(nodeName);
            visited.add(nodeName);
            log.warn("[{}] DAG '{}': 在循环检测中遇到未注册的节点 '{}'", contextType.getSimpleName(), getDagName(), nodeName);
            return;
        }

        List<String> deps = node.getDependencies().stream()
                .map(DependencyDescriptor::getName)
                .collect(Collectors.toList());

        for (String depName : deps) {
            if (!nodesByName.containsKey(depName)) {
                throw new IllegalStateException(String.format("[%s] DAG '%s': 节点 '%s' 依赖不存在的节点 '%s'",
                        contextType.getSimpleName(), getDagName(), nodeName, depName));
            }
            if (!visited.contains(depName)) {
                if (visiting.contains(depName)) {
                    throw new IllegalStateException(
                            String.format("[%s] DAG '%s': 检测到循环依赖涉及 '%s' 和 '%s'",
                                    contextType.getSimpleName(), getDagName(), depName, nodeName));
                }
                checkCycleDFS(depName, visiting, visited);
            }
        }
        visiting.remove(nodeName);
        visited.add(nodeName);
    }

    private List<String> calculateExecutionOrder() {
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> adj = new HashMap<>();

        for (String nodeName : nodesByName.keySet()) {
            inDegree.put(nodeName, 0);
            adj.put(nodeName, new ArrayList<>());
        }

        for (DagNode<C, ?> node : nodesByName.values()) {
            String dependerName = node.getName();
            for (DependencyDescriptor dep : node.getDependencies()) {
                String dependencyName = dep.getName();
                if (adj.containsKey(dependencyName)) {
                    adj.get(dependencyName).add(dependerName);
                    inDegree.put(dependerName, inDegree.get(dependerName) + 1);
                } else {
                    throw new IllegalStateException(String.format("[%s] DAG '%s': 在拓扑排序中发现未经验证的依赖 '%s' -> '%s'",
                            contextType.getSimpleName(), getDagName(), dependencyName, dependerName));
                }
            }
        }

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
            // 找到图中仍有入度的节点，它们构成了循环的一部分
            Set<String> remainingNodes = nodesByName.keySet().stream()
                    .filter(n -> !sortedOrder.contains(n))
                    .collect(Collectors.toSet());
            log.error("[{}] DAG '{}': 拓扑排序失败，图中可能存在循环。未排序节点: {}",
                    contextType.getSimpleName(), getDagName(), remainingNodes);
            throw new IllegalStateException(String.format("[%s] DAG '%s': 拓扑排序失败，检测到循环或图不连通。未排序节点: %s",
                    contextType.getSimpleName(), getDagName(), remainingNodes));
        }

        return Collections.unmodifiableList(sortedOrder);
    }

    private void printDagStructure() {
        if (!log.isInfoEnabled()) return;

        log.info("[{}] DAG '{}' 结构表示:", contextType.getSimpleName(), getDagName());
        StringBuilder builder = new StringBuilder("\n");
        builder.append(String.format("%-30s | %-25s | %-30s | %-30s\n",
                "节点名称 (输出类型)", "实现类", "依赖节点", "被依赖节点"));
        StringBuilder divider = new StringBuilder();
        for (int i = 0; i < 120; i++) divider.append("-"); // 调整分隔线长度
        builder.append(divider).append("\n");


        Map<String, Set<String>> dependsOn = new HashMap<>();
        Map<String, Set<String>> dependedBy = new HashMap<>();
        for (String nodeName : nodesByName.keySet()) {
            dependsOn.put(nodeName, new HashSet<>());
            dependedBy.put(nodeName, new HashSet<>());
        }
        for (DagNode<C, ?> node : getAllNodes()) {
            String nodeName = node.getName();
            for (DependencyDescriptor dep : node.getDependencies()) {
                String depName = dep.getName();
                dependsOn.computeIfAbsent(nodeName, k -> new HashSet<>()).add(depName);
                dependedBy.computeIfAbsent(depName, k -> new HashSet<>()).add(nodeName);
            }
        }

        for (String nodeName : executionOrder) {
            DagNode<C, ?> node = nodesByName.get(nodeName);
            if (node == null) continue;

            String nameAndType = String.format("%s (%s)", nodeName, node.getPayloadType().getSimpleName());
            String implClass = node.getClass().getSimpleName();
            String dependsOnStr = formatNodeSet(dependsOn.get(nodeName));
            String dependedByStr = formatNodeSet(dependedBy.get(nodeName));

            builder.append(String.format("%-30s | %-25s | %-30s | %-30s\n",
                    nameAndType, implClass, dependsOnStr, dependedByStr));
        }

        builder.append("\n执行路径 (→ 表示执行顺序):\n");
        builder.append(String.join(" → ", executionOrder));
        log.info(builder.toString());
    }

    private void generateDotGraph() {
        if (!log.isInfoEnabled()) return;

        StringBuilder dotGraph = new StringBuilder();
        dotGraph.append(String.format("digraph DAG_%s_%s {\n", contextType.getSimpleName(), getDagName().replaceAll("\\W+", "_")));
        dotGraph.append(String.format("  label=\"DAG Structure (%s - %s)\";\n", contextType.getSimpleName(), getDagName()));
        dotGraph.append("  rankdir=LR;\n");
        dotGraph.append("  node [shape=box, style=\"rounded,filled\", fontname=\"Arial\"];\n");

        for (String nodeName : nodesByName.keySet()) {
            DagNode<C, ?> node = nodesByName.get(nodeName);
            if (node == null) continue;
            String label = String.format("%s\\n(%s)\\n%s", nodeName, node.getPayloadType().getSimpleName(), node.getClass().getSimpleName());
            dotGraph.append(String.format("  \"%s\" [label=\"%s\"];\n", nodeName, label));
        }

        for (DagNode<C, ?> node : getAllNodes()) {
            String nodeName = node.getName();
            for (DependencyDescriptor dep : node.getDependencies()) {
                String depName = dep.getName();
                // 确保边连接的是存在的节点
                if (nodesByName.containsKey(depName) && nodesByName.containsKey(nodeName)) {
                    dotGraph.append(String.format("  \"%s\" -> \"%s\";\n", depName, nodeName));
                }
            }
        }
        dotGraph.append("}\n");
        log.info("[{}] DAG '{}' DOT格式图:\n{}", contextType.getSimpleName(), getDagName(), dotGraph.toString());
    }

    private String formatNodeSet(Set<String> nodeSet) {
        if (nodeSet == null || nodeSet.isEmpty()) return "无";
        return String.join(", ", nodeSet);
    }

    // --- DagDefinition 接口实现 ---

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<DagNode<C, T>> getNode(String nodeName, Class<T> payloadType) {
        Map<Class<?>, DagNode<C, ?>> typeMap = nodesByType.get(nodeName);
        if (typeMap == null) {
            return Optional.empty();
        }
        // 需要类型转换和检查
        DagNode<C, ?> node = typeMap.get(payloadType);
        if (node != null && payloadType.isAssignableFrom(node.getPayloadType())) {
            return Optional.of((DagNode<C, T>) node);
        }
        return Optional.empty();
    }

    @Override
    public Optional<DagNode<C, ?>> getNodeAnyType(String nodeName) {
        return Optional.ofNullable(nodesByName.get(nodeName));
    }

    @Override
    public Collection<DagNode<C, ?>> getAllNodes() {
        return Collections.unmodifiableCollection(nodesByName.values());
    }

    @Override
    public Set<Class<?>> getSupportedOutputTypes(String nodeName) {
        Map<Class<?>, DagNode<C, ?>> typeMap = nodesByType.get(nodeName);
        return (typeMap == null) ? Collections.emptySet() : Collections.unmodifiableSet(typeMap.keySet());
    }

    @Override
    public <T> boolean supportsOutputType(String nodeName, Class<T> payloadType) {
        return getNode(nodeName, payloadType).isPresent();
    }
}