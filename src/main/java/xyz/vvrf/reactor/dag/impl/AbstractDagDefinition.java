package xyz.vvrf.reactor.dag.impl;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * DagDefinition 的抽象基类，提供通用的节点管理、验证和拓扑排序逻辑。
 * <p>
 * 此实现强制要求 DAG 内的节点名称必须唯一。
 * 执行顺序依赖和输入数据映射通过外部 (如 ChainBuilder) 配置。
 * </p>
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen (Refactored by Devin)
 */
@Slf4j
public abstract class AbstractDagDefinition<C> implements DagDefinition<C> {

    private final Map<String, DagNode<C, ?, ?>> nodesByName = new ConcurrentHashMap<>();

    // 存储执行依赖：目标节点 -> [源节点列表]
    private Map<String, List<String>> executionDependencies = new ConcurrentHashMap<>();
    // 存储输入映射：目标节点 -> [输入需求 -> 源节点名称]
    private Map<String, Map<InputRequirement<?>, String>> inputMappings = new ConcurrentHashMap<>();
    // 存储反向执行依赖（邻接表）：源节点 -> [目标节点列表]
    private Map<String, List<String>> executionAdjacencyList = Collections.emptyMap();
    // 存储每个节点的直接执行前驱
    private Map<String, Set<String>> executionPredecessors = Collections.emptyMap();

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
            this.initialized = false;
        } else {
            try {
                registerNodes(nodes);
            } catch (IllegalStateException e) {
                log.error("[{}] DAG '{}' 节点注册失败: {}", contextType.getSimpleName(), getDagName(), e.getMessage());
                nodesByName.clear();
                this.initialized = false;
                throw e;
            }
        }
    }

    /**
     * 由 ChainBuilder 调用，设置执行依赖关系。
     * 会覆盖之前的设置。
     * @param execDeps 目标节点 -> [源节点列表] 的映射
     */
    public synchronized void setExecutionDependencies(Map<String, List<String>> execDeps) {
        if (initialized) {
            log.warn("[{}] DAG '{}' 已初始化，不应再修改执行依赖。", contextType.getSimpleName(), getDagName());
            return;
        }
        this.executionDependencies = new ConcurrentHashMap<>(Objects.requireNonNull(execDeps));
        log.info("[{}] DAG '{}': 已设置 {} 个节点的执行依赖。", contextType.getSimpleName(), getDagName(), execDeps.size());
    }

    /**
     * 由 ChainBuilder 调用，设置输入映射关系。
     * 会覆盖之前的设置。
     * @param inputMaps 目标节点 -> [输入需求 -> 源节点名称] 的映射
     */
    public synchronized void setInputMappings(Map<String, Map<InputRequirement<?>, String>> inputMaps) {
        if (initialized) {
            log.warn("[{}] DAG '{}' 已初始化，不应再修改输入映射。", contextType.getSimpleName(), getDagName());
            return;
        }
        this.inputMappings = new ConcurrentHashMap<>(Objects.requireNonNull(inputMaps));
        log.info("[{}] DAG '{}': 已设置 {} 个节点的输入映射。", contextType.getSimpleName(), getDagName(), inputMaps.size());
    }

    /**
     * 获取指定节点的输入映射配置。
     * @param nodeName 节点名称
     * @return 该节点的输入映射 (InputRequirement -> sourceNodeName)，如果无映射则为空 Map。
     */
    public Map<InputRequirement<?>, String> getInputMappingForNode(String nodeName) {
        return Collections.unmodifiableMap(inputMappings.getOrDefault(nodeName, Collections.emptyMap()));
    }

    /**
     * 获取指定节点的直接执行前驱节点名称集合。
     * @param nodeName 节点名称
     * @return 前驱节点名称集合，如果无前驱则为空 Set。
     */
    public Set<String> getExecutionPredecessors(String nodeName) {
        return Collections.unmodifiableSet(executionPredecessors.getOrDefault(nodeName, Collections.emptySet()));
    }


    @Override
    public synchronized void initialize() throws IllegalStateException {
        if (initialized) {
            log.debug("[{}] DAG '{}' 已初始化，跳过", contextType.getSimpleName(), getDagName());
            return;
        }
        if (nodesByName.isEmpty()) {
            log.warn("[{}] DAG '{}' 为空 (无节点)，初始化完成。", contextType.getSimpleName(), getDagName());
            this.executionOrder = Collections.emptyList();
            this.executionAdjacencyList = Collections.emptyMap();
            this.executionPredecessors = Collections.emptyMap();
            this.initialized = true;
            return;
        }

        log.info("[{}] 开始初始化和验证 DAG '{}' (基于执行依赖和输入映射)...", contextType.getSimpleName(), getDagName());
        try {
            // 1. 构建邻接表和计算入度 (基于 executionDependencies)
            buildGraphStructures();

            // 2. 检测执行依赖中的循环
            detectCycles();

            // 3. 计算拓扑执行顺序
            this.executionOrder = calculateExecutionOrder();

            // 4. 验证输入映射 (必须在拓扑排序后进行，以检查前驱关系)
            validateInputMappings();

            this.initialized = true;

            log.info("[{}] DAG '{}' 初始化和验证成功", contextType.getSimpleName(), getDagName());
            log.info("[{}] DAG '{}' 执行顺序: {}", contextType.getSimpleName(), getDagName(), executionOrder);
            printDagStructure(); // 更新打印逻辑
            generateDotGraph(); // 更新图生成逻辑
        } catch (IllegalStateException e) {
            log.error("[{}] DAG '{}' 初始化或验证失败: {}", contextType.getSimpleName(), getDagName(), e.getMessage());
            this.executionOrder = Collections.emptyList();
            this.executionAdjacencyList = Collections.emptyMap();
            this.executionPredecessors = Collections.emptyMap();
            this.initialized = false;
            throw e;
        }
    }

    private void buildGraphStructures() {
        log.debug("[{}] DAG '{}': 构建执行图结构...", contextType.getSimpleName(), getDagName());
        Map<String, List<String>> adjList = new ConcurrentHashMap<>();
        Map<String, Set<String>> predecessors = new ConcurrentHashMap<>();

        // 初始化所有注册节点的邻接表和前驱集合
        for (String nodeName : nodesByName.keySet()) {
            adjList.put(nodeName, Collections.synchronizedList(new ArrayList<>()));
            predecessors.put(nodeName, Collections.synchronizedSet(new HashSet<>()));
        }

        // 根据 executionDependencies 构建邻接表和前驱集合
        for (Map.Entry<String, List<String>> entry : executionDependencies.entrySet()) {
            String targetNode = entry.getKey();
            List<String> sourceNodes = entry.getValue();

            if (!nodesByName.containsKey(targetNode)) {
                log.warn("[{}] DAG '{}': 执行依赖中目标节点 '{}' 未注册，忽略此依赖。", contextType.getSimpleName(), getDagName(), targetNode);
                continue;
            }

            if (sourceNodes != null) {
                for (String sourceNode : sourceNodes) {
                    if (!nodesByName.containsKey(sourceNode)) {
                        log.warn("[{}] DAG '{}': 执行依赖中源节点 '{}' (为 '{}' 的依赖) 未注册，忽略此依赖。", contextType.getSimpleName(), getDagName(), sourceNode, targetNode);
                        continue;
                    }
                    // 添加边 source -> target
                    adjList.computeIfAbsent(sourceNode, k -> Collections.synchronizedList(new ArrayList<>())).add(targetNode);
                    // 添加 target 的前驱 source
                    predecessors.computeIfAbsent(targetNode, k -> Collections.synchronizedSet(new HashSet<>())).add(sourceNode);
                }
            }
        }
        this.executionAdjacencyList = Collections.unmodifiableMap(adjList);
        this.executionPredecessors = Collections.unmodifiableMap(predecessors);
        log.debug("[{}] DAG '{}': 执行图结构构建完成。", contextType.getSimpleName(), getDagName());
    }


    private void detectCycles() {
        log.debug("[{}] DAG '{}': 检测执行依赖中的循环...", contextType.getSimpleName(), getDagName());
        Set<String> visited = new HashSet<>(); // 完全访问过的节点
        Set<String> visiting = new HashSet<>(); // 当前递归路径上的节点

        for (String nodeName : nodesByName.keySet()) {
            if (!visited.contains(nodeName)) {
                checkCycleDFS(nodeName, visiting, visited);
            }
        }
        log.info("[{}] DAG '{}': 未检测到执行依赖循环", contextType.getSimpleName(), getDagName());
    }

    private void checkCycleDFS(String nodeName, Set<String> visiting, Set<String> visited) {
        visiting.add(nodeName);

        List<String> neighbors = executionAdjacencyList.getOrDefault(nodeName, Collections.emptyList());
        for (String neighbor : neighbors) {
            if (visiting.contains(neighbor)) {
                // 发现循环
                throw new IllegalStateException(
                        String.format("[%s] DAG '%s': 检测到执行依赖循环！路径涉及 '%s' -> '%s' (可能更长)",
                                contextType.getSimpleName(), getDagName(), neighbor, nodeName));
            }
            if (!visited.contains(neighbor)) {
                checkCycleDFS(neighbor, visiting, visited);
            }
        }

        visiting.remove(nodeName);
        visited.add(nodeName);
    }

    private List<String> calculateExecutionOrder() {
        log.debug("[{}] DAG '{}': 计算拓扑执行顺序...", contextType.getSimpleName(), getDagName());
        Map<String, Integer> inDegree = new HashMap<>();
        Queue<String> queue = new LinkedList<>();
        List<String> sortedOrder = new ArrayList<>();

        // 计算所有节点的入度
        for (String nodeName : nodesByName.keySet()) {
            inDegree.put(nodeName, executionPredecessors.getOrDefault(nodeName, Collections.emptySet()).size());
            if (inDegree.get(nodeName) == 0) {
                // 入度为0的节点入队
                queue.offer(nodeName);
            }
        }

        while (!queue.isEmpty()) {
            String u = queue.poll();
            sortedOrder.add(u);

            // 将 u 的所有邻居的入度减 1
            for (String v : executionAdjacencyList.getOrDefault(u, Collections.emptyList())) {
                inDegree.put(v, inDegree.get(v) - 1);
                if (inDegree.get(v) == 0) {
                    // 如果邻居入度变为0，入队
                    queue.offer(v);
                }
            }
        }

        if (sortedOrder.size() != nodesByName.size()) {
            Set<String> remainingNodes = nodesByName.keySet().stream()
                    .filter(n -> !sortedOrder.contains(n))
                    .collect(Collectors.toSet());
            log.error("[{}] DAG '{}': 拓扑排序失败，图中可能存在循环或节点不可达。未排序节点: {}",
                    contextType.getSimpleName(), getDagName(), remainingNodes);
            // 循环应该在 detectCycles 中被捕获，这里更可能是节点不可达（如果没有执行依赖指向它，且它不是起点）
            // 或者是有节点注册了但没有被包含在任何执行依赖中。
            // 允许这种情况？还是强制所有节点必须在执行图中？ 暂时允许，只对能排序的部分排序。
            // 但如果 input mapping 引用了未排序的节点，后面会报错。
            // 严格模式：如果排序结果数量不等于节点数量，则抛异常。
            throw new IllegalStateException(String.format("[%s] DAG '%s': 拓扑排序失败，排序节点数 (%d) 与总节点数 (%d) 不匹配。可能存在循环或孤立节点。未排序节点: %s",
                    contextType.getSimpleName(), getDagName(), sortedOrder.size(), nodesByName.size(), remainingNodes));
        }

        return Collections.unmodifiableList(sortedOrder);
    }

    private void validateInputMappings() {
        log.debug("[{}] DAG '{}': 验证输入映射...", contextType.getSimpleName(), getDagName());
        Set<String> executionOrderSet = new HashSet<>(this.executionOrder);

        for (String targetNodeName : this.executionOrder) {
            DagNode<C, ?, ?> targetNode = nodesByName.get(targetNodeName);
            if (targetNode == null) continue;

            List<InputRequirement<?>> requirements = targetNode.getInputRequirements();
            Map<InputRequirement<?>, String> nodeMappings = inputMappings.getOrDefault(targetNodeName, Collections.emptyMap());
            Set<String> predecessors = executionPredecessors.getOrDefault(targetNodeName, Collections.emptySet());

            // 检查每个输入需求
            for (InputRequirement<?> req : requirements) {
                String sourceNodeName = nodeMappings.get(req);

                if (sourceNodeName == null) {
                    // 没有显式映射，尝试自动解析 (如果无限定符且只有一个前驱提供该类型)
                    if (!req.getQualifier().isPresent()) {
                        List<String> potentialSources = predecessors.stream()
                                .map(nodesByName::get)
                                .filter(Objects::nonNull)
                                .filter(p -> req.getType().isAssignableFrom(p.getPayloadType()))
                                .map(DagNode::getName)
                                .collect(Collectors.toList());

                        if (potentialSources.size() == 1) {
                            sourceNodeName = potentialSources.get(0);
                            log.debug("[{}] DAG '{}': 节点 '{}' 的输入需求 {} 自动解析到源 '{}'",
                                    contextType.getSimpleName(), getDagName(), targetNodeName, req, sourceNodeName);
                            // 注意并发修改问题，最好在构建时完成
                             nodeMappings.put(req, sourceNodeName);
                        } else if (potentialSources.size() > 1) {
                            if (!req.isOptional()) {
                                throw new IllegalStateException(
                                        String.format("[%s] DAG '%s': 节点 '%s' 的必需输入需求 %s 存在歧义，多个前驱节点 (%s) 提供兼容类型，需要显式 mapInput()。",
                                                contextType.getSimpleName(), getDagName(), targetNodeName, req, potentialSources));
                            } else {
                                log.warn("[{}] DAG '{}': 节点 '{}' 的可选输入需求 {} 存在歧义 (%s)，将无法自动获取，返回 Optional.empty()。",
                                        contextType.getSimpleName(), getDagName(), targetNodeName, req, potentialSources);
                                // 标记为无法满足，即使是可选的
                                sourceNodeName = null;
                            }
                        } else {
                            // 没有找到源
                            if (!req.isOptional()) {
                                throw new IllegalStateException(
                                        String.format("[%s] DAG '%s': 节点 '%s' 的必需输入需求 %s 没有找到兼容的源节点或显式映射。",
                                                contextType.getSimpleName(), getDagName(), targetNodeName, req));
                            } else {
                                log.debug("[{}] DAG '{}': 节点 '{}' 的可选输入需求 {} 未找到源或映射，将返回 Optional.empty()。",
                                        contextType.getSimpleName(), getDagName(), targetNodeName, req);
                                sourceNodeName = null;
                            }
                        }
                    } else {
                        // 有限定符但没有映射
                        if (!req.isOptional()) {
                            throw new IllegalStateException(
                                    String.format("[%s] DAG '%s': 节点 '%s' 的必需输入需求 %s (带限定符) 没有显式映射。",
                                            contextType.getSimpleName(), getDagName(), targetNodeName, req));
                        } else {
                            log.debug("[{}] DAG '{}': 节点 '{}' 的可选输入需求 {} (带限定符) 没有显式映射，将返回 Optional.empty()。",
                                    contextType.getSimpleName(), getDagName(), targetNodeName, req);
                            sourceNodeName = null;
                        }
                    }
                }

                // 如果找到了映射源 (显式或自动)
                if (sourceNodeName != null) {
                    // 验证源节点是否存在
                    DagNode<C, ?, ?> sourceNode = nodesByName.get(sourceNodeName);
                    if (sourceNode == null) {
                        throw new IllegalStateException(
                                String.format("[%s] DAG '%s': 节点 '%s' 的输入需求 %s 映射到的源节点 '%s' 不存在。",
                                        contextType.getSimpleName(), getDagName(), targetNodeName, req, sourceNodeName));
                    }
                    // 验证源节点是否是执行前驱 (基于拓扑排序结果)
                    // 注意：这里检查的是直接前驱。如果允许间接前驱提供数据，逻辑会更复杂。
                    // 假设：数据只能由直接执行前驱提供。
                    if (!predecessors.contains(sourceNodeName)) {
                        throw new IllegalStateException(
                                String.format("[%s] DAG '%s': 节点 '%s' 的输入需求 %s 映射到的源节点 '%s' 不是其直接执行前驱。",
                                        contextType.getSimpleName(), getDagName(), targetNodeName, req, sourceNodeName));
                    }

                    // 验证源节点输出类型是否兼容 (ChainBuilder 已检查，这里再次确认)
                    if (!req.getType().isAssignableFrom(sourceNode.getPayloadType())) {
                        throw new IllegalStateException(
                                String.format("[%s] DAG '%s': 节点 '%s' 的输入需求 %s (类型 %s) 与源节点 '%s' 的输出类型 (%s) 不兼容。",
                                        contextType.getSimpleName(), getDagName(), targetNodeName, req, req.getType().getSimpleName(),
                                        sourceNodeName, sourceNode.getPayloadType().getSimpleName()));
                    }
                } else if (!req.isOptional()) {
                    // 如果是必需输入，到这里还没找到源，说明有问题（前面的逻辑应该已经抛异常了）
                    throw new IllegalStateException(
                            String.format("[%s] DAG '%s': 内部错误：节点 '%s' 的必需输入需求 %s 在验证结束时仍未找到源。",
                                    contextType.getSimpleName(), getDagName(), targetNodeName, req));
                }
            }
        }
        log.info("[{}] DAG '{}': 输入映射验证通过。", contextType.getSimpleName(), getDagName());
    }


    @Override
    public Class<C> getContextType() {
        return contextType;
    }

    @Override
    public <P> Optional<DagNode<C, P, ?>> getNode(String nodeName, Class<P> payloadType) {
        DagNode<C, ?, ?> node = nodesByName.get(nodeName);
        if (node == null) {
            return Optional.empty();
        }
        // 检查节点声明的输出类型是否与请求的类型兼容
        if (payloadType.isAssignableFrom(node.getPayloadType())) {
            // 类型兼容，进行强制转换
            // 这是安全的，因为我们检查了 isAssignableFrom
            @SuppressWarnings("unchecked")
            DagNode<C, P, ?> typedNode = (DagNode<C, P, ?>) node;
            return Optional.of(typedNode);
        } else {
            log.warn("[{}] DAG '{}': 请求节点 '{}' 的 Payload 类型 '{}'，但该节点实际 Payload 类型为 '{}' (不兼容)",
                    contextType.getSimpleName(), getDagName(), nodeName,
                    payloadType.getSimpleName(), node.getPayloadType().getSimpleName());
            return Optional.empty();
        }
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
    public List<String> getExecutionOrder() {
        if (!initialized) {
            log.warn("[{}] DAG '{}' 尚未初始化，执行顺序可能不准确或为空。", contextType.getSimpleName(), getDagName());
        }
        return executionOrder;
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
                // 检查节点声明的输出类型是否能赋值给请求的类型
                .map(node -> payloadType.isAssignableFrom(node.getPayloadType()))
                .orElse(false);
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    private void registerNodes(List<DagNode<C, ?, ?>> nodes) throws IllegalStateException {
        log.debug("[{}] DAG '{}': 开始注册 {} 个提供的节点...", contextType.getSimpleName(), getDagName(), nodes.size());
        for (DagNode<C, ?, ?> node : nodes) {
            String nodeName = node.getName();
            Class<?> payloadType = node.getPayloadType();
            Class<?> eventType = node.getEventType();

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
            if (eventType == null) {
                String errorMsg = String.format("[%s] DAG '%s': 节点 '%s' (实现类: %s) 未提供有效的 Event 类型 (getEventType() 返回 null)。",
                        contextType.getSimpleName(), getDagName(), nodeName, node.getClass().getName());
                log.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            if (nodesByName.containsKey(nodeName)) {
                DagNode<C, ?, ?> existingNode = nodesByName.get(nodeName);
                String errorMsg = String.format(
                        "[%s] DAG '%s': 节点名称冲突！名称 '%s' 已被节点 '%s' (类: %s) 使用，不能再被节点 '%s' (类: %s) 使用。节点名称在 DAG 中必须唯一。",
                        contextType.getSimpleName(), getDagName(), nodeName,
                        existingNode.getName(),
                        existingNode.getClass().getName(),
                        node.getName(),
                        node.getClass().getName()
                );
                log.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }

            nodesByName.put(nodeName, node);

            log.info("[{}] DAG '{}': 已注册节点: '{}' (Payload: {}, Event: {}, Impl: {})",
                    contextType.getSimpleName(), getDagName(), nodeName,
                    payloadType.getSimpleName(), eventType.getSimpleName(),
                    node.getClass().getSimpleName());
        }
        log.info("[{}] DAG '{}' 节点注册完成，共 {} 个唯一名称的节点", contextType.getSimpleName(), getDagName(), nodesByName.size());
    }

    private void printDagStructure() {
        if (!log.isInfoEnabled() || nodesByName.isEmpty()) return;

        log.info("[{}] DAG '{}' 结构表示 (基于执行依赖和输入映射):", contextType.getSimpleName(), getDagName());
        StringBuilder builder = new StringBuilder("\n");
        builder.append(String.format("%-35s | %-30s | %-50s | %-50s | %-60s\n",
                "节点 (Payload, Event)", "实现类", "执行前驱", "执行后继", "输入映射 (需求 -> 源节点)"));
        StringBuilder divider = new StringBuilder();
        for (int i = 0; i < 230; i++) divider.append("-");
        builder.append(divider).append("\n");

        List<String> nodesToIterate = this.executionOrder.isEmpty() ?
                new ArrayList<>(nodesByName.keySet()) : this.executionOrder;
        Collections.sort(nodesToIterate);

        if (nodesToIterate.isEmpty()) {
            builder.append("  (DAG 为空)\n");
        } else {
            for (String nodeName : nodesToIterate) {
                DagNode<C, ?, ?> node = nodesByName.get(nodeName);
                if (node == null) continue;

                String nameAndTypes = String.format("%s (%s, %s)", nodeName,
                        node.getPayloadType().getSimpleName(), node.getEventType().getSimpleName());
                String implClass = node.getClass().getSimpleName();
                String predecessorsStr = formatNodeSet(executionPredecessors.getOrDefault(nodeName, Collections.emptySet()));
                String successorsStr = formatNodeSet(new HashSet<>(executionAdjacencyList.getOrDefault(nodeName, Collections.emptyList()))); // 转 Set 去重再格式化

                // 格式化输入映射
                Map<InputRequirement<?>, String> mappings = inputMappings.getOrDefault(nodeName, Collections.emptyMap());
                String mappingsStr;
                if (mappings.isEmpty()) {
                    mappingsStr = "无";
                } else {
                    mappingsStr = mappings.entrySet().stream()
                            .map(entry -> String.format("%s -> %s", entry.getKey(), entry.getValue()))
                            .collect(Collectors.joining(", "));
                }

                builder.append(String.format("%-35s | %-30s | %-50s | %-50s | %-60s\n",
                        nameAndTypes, implClass, predecessorsStr, successorsStr, mappingsStr));
            }
        }

        if (!executionOrder.isEmpty()) {
            builder.append("\n执行路径 (→ 表示执行顺序):\n");
            builder.append(String.join(" → ", executionOrder));
        } else if (!nodesByName.isEmpty()) {
            builder.append("\n(无有效执行路径 - 可能存在循环、节点不可达或未初始化)\n");
        }

        log.info(builder.toString());
    }

    private void generateDotGraph() {
        if (!log.isInfoEnabled() || nodesByName.isEmpty()) return;

        StringBuilder dotGraph = new StringBuilder();
        String graphName = String.format("DAG_%s_%s", contextType.getSimpleName(), getDagName().replaceAll("\\W+", "_"));
        dotGraph.append(String.format("digraph %s {\n", graphName));
        dotGraph.append(String.format("  label=\"DAG Structure (%s - %s) - Execution & Input Mapping\";\n", contextType.getSimpleName(), getDagName()));
        dotGraph.append("  labelloc=top;\n");
        dotGraph.append("  fontsize=16;\n");
        dotGraph.append("  rankdir=LR;\n");
        dotGraph.append("  node [shape=record, style=\"rounded,filled\", fillcolor=\"lightblue\", fontname=\"Arial\", fontsize=10];\n");
        dotGraph.append("  edge [fontname=\"Arial\", fontsize=9];\n\n");

        // 节点定义 (包含输入需求)
        for (String nodeName : nodesByName.keySet()) {
            DagNode<C, ?, ?> node = nodesByName.get(nodeName);
            if (node == null) continue;
            List<InputRequirement<?>> inputs = node.getInputRequirements();
            String inputLabel = inputs.isEmpty() ? "" : inputs.stream()
                    .map(InputRequirement::toString)
                    .collect(Collectors.joining("\\n"));

            String label = String.format("{%s | {Payload: %s | Event: %s} | {Inputs:\\n%s}}",
                    nodeName,
                    node.getPayloadType().getSimpleName(),
                    node.getEventType().getSimpleName(),
                    inputLabel.isEmpty() ? " (none)" : inputLabel);
            dotGraph.append(String.format("  \"%s\" [label=\"%s\"];\n", nodeName, label));
        }
        dotGraph.append("\n");

        // 执行依赖边 (实线)
        dotGraph.append("  // Execution Edges\n");
        for (Map.Entry<String, List<String>> entry : executionAdjacencyList.entrySet()) {
            String source = entry.getKey();
            for (String target : entry.getValue()) {
                dotGraph.append(String.format("  \"%s\" -> \"%s\" [style=solid, color=black];\n", source, target));
            }
        }
        dotGraph.append("\n");

        // 输入映射边 (虚线，带标签)
        dotGraph.append("  // Input Mapping Edges\n");
        for (Map.Entry<String, Map<InputRequirement<?>, String>> targetEntry : inputMappings.entrySet()) {
            String target = targetEntry.getKey();
            for (Map.Entry<InputRequirement<?>, String> mappingEntry : targetEntry.getValue().entrySet()) {
                InputRequirement<?> req = mappingEntry.getKey();
                String source = mappingEntry.getValue();
                String edgeLabel = String.format("req: %s\\n(type: %s%s)",
                        req.getQualifier().orElse("(default)"),
                        req.getType().getSimpleName(),
                        req.isOptional() ? ", opt" : "");
                dotGraph.append(String.format("  \"%s\" -> \"%s\" [style=dashed, color=blue, label=\"%s\"];\n",
                        source, target, edgeLabel));
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

}
