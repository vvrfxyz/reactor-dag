package xyz.vvrf.reactor.dag.impl;

import lombok.Getter; // 移除，因为没有字段需要它
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
 * <p>
 * **修改**: 输入映射验证不再强制要求源节点是直接执行前驱。
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
    // 存储每个节点的直接执行后继
    private Map<String, Set<String>> executionSuccessors = Collections.emptyMap(); // 新增

    private List<String> executionOrder = Collections.emptyList();

    private final Class<C> contextType;
    private volatile boolean initialized = false; // 使用 volatile 保证可见性

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
            this.initialized = false; // 显式设置
        } else {
            try {
                registerNodes(nodes);
            } catch (IllegalStateException e) {
                log.error("[{}] DAG '{}' 节点注册失败: {}", contextType.getSimpleName(), getDagName(), e.getMessage());
                nodesByName.clear();
                this.initialized = false; // 显式设置
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
        // 深度复制以确保不可变性
        Map<String, List<String>> copiedDeps = new ConcurrentHashMap<>();
        execDeps.forEach((key, value) -> copiedDeps.put(key, Collections.unmodifiableList(new ArrayList<>(value))));
        this.executionDependencies = copiedDeps;
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
        // 深度复制
        Map<String, Map<InputRequirement<?>, String>> copiedMaps = new ConcurrentHashMap<>();
        inputMaps.forEach((node, mappings) -> copiedMaps.put(node, new ConcurrentHashMap<>(mappings)));
        this.inputMappings = copiedMaps;
        log.info("[{}] DAG '{}': 已设置 {} 个节点的输入映射。", contextType.getSimpleName(), getDagName(), inputMaps.size());
    }

    @Override
    public Map<InputRequirement<?>, String> getInputMappingForNode(String nodeName) {
        return Collections.unmodifiableMap(inputMappings.getOrDefault(nodeName, Collections.emptyMap()));
    }

    @Override
    public Set<String> getExecutionPredecessors(String nodeName) {
        return Collections.unmodifiableSet(executionPredecessors.getOrDefault(nodeName, Collections.emptySet()));
    }

    @Override
    public Set<String> getExecutionSuccessors(String nodeName) {
        return Collections.unmodifiableSet(executionSuccessors.getOrDefault(nodeName, Collections.emptySet()));
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
            this.executionSuccessors = Collections.emptyMap();
            this.initialized = true;
            return;
        }

        log.info("[{}] 开始初始化和验证 DAG '{}' (基于执行依赖和输入映射)...", contextType.getSimpleName(), getDagName());
        try {
            // 1. 构建邻接表、计算前驱和后继 (基于 executionDependencies)
            buildGraphStructures();

            // 2. 检测执行依赖中的循环
            detectCycles();

            // 3. 计算拓扑执行顺序
            this.executionOrder = calculateExecutionOrder();

            // 4. 验证输入映射 (不再检查是否为直接前驱)
            validateInputMappings();

            this.initialized = true; // 设置 initialized 标志

            log.info("[{}] DAG '{}' 初始化和验证成功", contextType.getSimpleName(), getDagName());
            log.info("[{}] DAG '{}' 执行顺序: {}", contextType.getSimpleName(), getDagName(), executionOrder);
            printDagStructure();
            generateDotGraph();
        } catch (IllegalStateException e) {
            log.error("[{}] DAG '{}' 初始化或验证失败: {}", contextType.getSimpleName(), getDagName(), e.getMessage(), e); // Log stacktrace
            // 清理状态
            this.executionOrder = Collections.emptyList();
            this.executionAdjacencyList = Collections.emptyMap();
            this.executionPredecessors = Collections.emptyMap();
            this.executionSuccessors = Collections.emptyMap();
            this.initialized = false; // 确保状态为未初始化
            throw e; // 重新抛出异常
        }
    }

    private void buildGraphStructures() {
        log.debug("[{}] DAG '{}': 构建执行图结构...", contextType.getSimpleName(), getDagName());
        Map<String, List<String>> adjList = new ConcurrentHashMap<>(); // source -> [targets]
        Map<String, Set<String>> predecessors = new ConcurrentHashMap<>(); // target -> {sources}
        Map<String, Set<String>> successors = new ConcurrentHashMap<>(); // source -> {targets}

        // 初始化所有注册节点的集合
        for (String nodeName : nodesByName.keySet()) {
            adjList.put(nodeName, Collections.synchronizedList(new ArrayList<>()));
            predecessors.put(nodeName, Collections.synchronizedSet(new HashSet<>()));
            successors.put(nodeName, Collections.synchronizedSet(new HashSet<>()));
        }

        // 根据 executionDependencies 构建图结构
        for (Map.Entry<String, List<String>> entry : executionDependencies.entrySet()) {
            String targetNode = entry.getKey();
            List<String> sourceNodes = entry.getValue();

            if (!nodesByName.containsKey(targetNode)) {
                log.warn("[{}] DAG '{}': 执行依赖中目标节点 '{}' 未注册，忽略此依赖。", contextType.getSimpleName(), getDagName(), targetNode);
                continue; // 跳过这个依赖关系
            }

            if (sourceNodes != null) {
                for (String sourceNode : sourceNodes) {
                    if (!nodesByName.containsKey(sourceNode)) {
                        log.warn("[{}] DAG '{}': 执行依赖中源节点 '{}' (为 '{}' 的依赖) 未注册，忽略此依赖。", contextType.getSimpleName(), getDagName(), sourceNode, targetNode);
                        continue; // 跳过这个特定的源
                    }
                    // 添加边 source -> target
                    adjList.computeIfAbsent(sourceNode, k -> Collections.synchronizedList(new ArrayList<>())).add(targetNode);
                    // 添加 target 的前驱 source
                    predecessors.computeIfAbsent(targetNode, k -> Collections.synchronizedSet(new HashSet<>())).add(sourceNode);
                    // 添加 source 的后继 target
                    successors.computeIfAbsent(sourceNode, k -> Collections.synchronizedSet(new HashSet<>())).add(targetNode);
                }
            }
        }
        // 设置为不可修改的视图
        this.executionAdjacencyList = Collections.unmodifiableMap(adjList);
        this.executionPredecessors = Collections.unmodifiableMap(predecessors);
        this.executionSuccessors = Collections.unmodifiableMap(successors);
        log.debug("[{}] DAG '{}': 执行图结构构建完成。", contextType.getSimpleName(), getDagName());
    }


    private void detectCycles() {
        log.debug("[{}] DAG '{}': 检测执行依赖中的循环...", contextType.getSimpleName(), getDagName());
        Set<String> visited = new HashSet<>(); // 完全访问过的节点
        Set<String> visiting = new HashSet<>(); // 当前递归路径上的节点
        List<String> path = new ArrayList<>(); // 用于记录循环路径

        for (String nodeName : nodesByName.keySet()) {
            if (!visited.contains(nodeName)) {
                path.clear(); // 开始新的 DFS 路径
                checkCycleDFS(nodeName, visiting, visited, path);
            }
        }
        log.info("[{}] DAG '{}': 未检测到执行依赖循环", contextType.getSimpleName(), getDagName());
    }

    private void checkCycleDFS(String nodeName, Set<String> visiting, Set<String> visited, List<String> path) {
        visiting.add(nodeName);
        path.add(nodeName); // 将当前节点加入路径

        List<String> neighbors = executionAdjacencyList.getOrDefault(nodeName, Collections.emptyList());
        for (String neighbor : neighbors) {
            if (visiting.contains(neighbor)) {
                // 发现循环
                int cycleStartIndex = path.indexOf(neighbor);
                String cyclePath = path.subList(cycleStartIndex, path.size()).stream()
                        .collect(Collectors.joining(" -> ")) + " -> " + neighbor;
                throw new IllegalStateException(
                        String.format("[%s] DAG '%s': 检测到执行依赖循环！路径: %s",
                                contextType.getSimpleName(), getDagName(), cyclePath));
            }
            if (!visited.contains(neighbor)) {
                checkCycleDFS(neighbor, visiting, visited, path);
            }
        }

        path.remove(path.size() - 1); // 回溯：从路径中移除当前节点
        visiting.remove(nodeName);
        visited.add(nodeName);
    }

    private List<String> calculateExecutionOrder() {
        log.debug("[{}] DAG '{}': 计算拓扑执行顺序...", contextType.getSimpleName(), getDagName());
        Map<String, Integer> inDegree = new HashMap<>();
        Queue<String> queue = new LinkedList<>();
        List<String> sortedOrder = new ArrayList<>();
        Set<String> allRegisteredNodes = new HashSet<>(nodesByName.keySet()); // 所有注册的节点

        // 计算所有注册节点的入度
        for (String nodeName : allRegisteredNodes) {
            int degree = executionPredecessors.getOrDefault(nodeName, Collections.emptySet()).size();
            inDegree.put(nodeName, degree);
            if (degree == 0) {
                // 入度为0的节点入队
                queue.offer(nodeName);
            }
        }

        while (!queue.isEmpty()) {
            String u = queue.poll();
            sortedOrder.add(u);

            // 将 u 的所有邻居（执行后继）的入度减 1
            for (String v : executionSuccessors.getOrDefault(u, Collections.emptySet())) {
                inDegree.computeIfPresent(v, (key, degree) -> {
                    int newDegree = degree - 1;
                    if (newDegree == 0) {
                        // 如果邻居入度变为0，入队
                        queue.offer(v);
                    }
                    return newDegree;
                });
            }
        }

        // 验证是否所有注册节点都被排序了
        if (sortedOrder.size() != allRegisteredNodes.size()) {
            Set<String> remainingNodes = new HashSet<>(allRegisteredNodes);
            remainingNodes.removeAll(sortedOrder);
            log.error("[{}] DAG '{}': 拓扑排序失败，图中可能存在循环或节点不可达。未排序节点: {}",
                    contextType.getSimpleName(), getDagName(), remainingNodes);
            // 循环应该在 detectCycles 中被捕获。这里通常意味着有节点注册了但未包含在执行图中（孤立节点）。
            // 严格模式：如果排序结果数量不等于节点数量，则抛异常。
            throw new IllegalStateException(String.format("[%s] DAG '%s': 拓扑排序失败，排序节点数 (%d) 与总注册节点数 (%d) 不匹配。可能存在未处理的循环或孤立节点。未排序节点: %s",
                    contextType.getSimpleName(), getDagName(), sortedOrder.size(), allRegisteredNodes.size(), remainingNodes));
        }

        return Collections.unmodifiableList(sortedOrder);
    }

    private void validateInputMappings() {
        log.debug("[{}] DAG '{}': 验证输入映射...", contextType.getSimpleName(), getDagName());

        for (String targetNodeName : nodesByName.keySet()) { // 遍历所有节点检查其输入
            DagNode<C, ?, ?> targetNode = nodesByName.get(targetNodeName);
            if (targetNode == null) continue; // 不应发生，但做防御性检查

            List<InputRequirement<?>> requirements = targetNode.getInputRequirements();
            Map<InputRequirement<?>, String> nodeMappings = inputMappings.getOrDefault(targetNodeName, Collections.emptyMap());

            // 检查每个输入需求
            for (InputRequirement<?> req : requirements) {
                String sourceNodeName = nodeMappings.get(req);

                if (sourceNodeName == null) {
                    // 没有显式映射，尝试自动解析 (仅当无限定符时)
                    if (!req.getQualifier().isPresent()) {
                        // 查找所有执行前驱中，类型兼容且无限定符的唯一节点
                        List<String> potentialSources = executionPredecessors.getOrDefault(targetNodeName, Collections.emptySet()).stream()
                                .map(nodesByName::get)
                                .filter(Objects::nonNull)
                                .filter(p -> req.getType().isAssignableFrom(p.getPayloadType()))
                                // 检查这个潜在源节点是否也有限定符的输出（这里简化，假设节点只有一个输出）
                                .map(DagNode::getName)
                                .collect(Collectors.toList());

                        if (potentialSources.size() == 1) {
                            sourceNodeName = potentialSources.get(0);
                            log.debug("[{}] DAG '{}': 节点 '{}' 的输入需求 {} 自动解析到直接前驱源 '{}'",
                                    contextType.getSimpleName(), getDagName(), targetNodeName, req, sourceNodeName);
                            // 动态添加自动解析的映射，以便后续使用
                            inputMappings.computeIfAbsent(targetNodeName, k -> new ConcurrentHashMap<>()).put(req, sourceNodeName);
                        } else if (potentialSources.size() > 1) {
                            // 存在多个直接前驱提供兼容类型，无法自动解析
                            if (!req.isOptional()) {
                                throw new IllegalStateException(
                                        String.format("[%s] DAG '%s': 节点 '%s' 的必需输入需求 %s 存在歧义，多个直接执行前驱 (%s) 提供兼容类型，需要使用 mapInput() 显式映射。",
                                                contextType.getSimpleName(), getDagName(), targetNodeName, req, potentialSources));
                            } else {
                                log.warn("[{}] DAG '{}': 节点 '{}' 的可选输入需求 {} 存在歧义 (%s)，无法自动解析，运行时将返回 Optional.empty()。",
                                        contextType.getSimpleName(), getDagName(), targetNodeName, req, potentialSources);
                                // 标记为无法满足，即使是可选的
                                sourceNodeName = null; // 确保后续检查知道没有找到源
                            }
                        } else {
                            // 没有直接前驱提供兼容类型
                            if (!req.isOptional()) {
                                // 对于必需输入，如果没有任何直接前驱提供，也需要显式映射（可能来自间接依赖）
                                throw new IllegalStateException(
                                        String.format("[%s] DAG '%s': 节点 '%s' 的必需输入需求 %s 没有找到兼容的直接执行前驱，需要使用 mapInput() 显式映射（可能映射到间接依赖）。",
                                                contextType.getSimpleName(), getDagName(), targetNodeName, req));
                            } else {
                                log.debug("[{}] DAG '{}': 节点 '{}' 的可选输入需求 {} 未找到兼容的直接执行前驱，将返回 Optional.empty()。",
                                        contextType.getSimpleName(), getDagName(), targetNodeName, req);
                                sourceNodeName = null;
                            }
                        }
                    } else {
                        // 有限定符但没有显式映射
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

                // 如果找到了映射源 (显式或自动解析的)
                if (sourceNodeName != null) {
                    // 验证源节点是否存在
                    DagNode<C, ?, ?> sourceNode = nodesByName.get(sourceNodeName);
                    if (sourceNode == null) {
                        throw new IllegalStateException(
                                String.format("[%s] DAG '%s': 节点 '%s' 的输入需求 %s 映射到的源节点 '%s' 未在 DAG 中注册。",
                                        contextType.getSimpleName(), getDagName(), targetNodeName, req, sourceNodeName));
                    }

                    // 现在只验证类型兼容性（ChainBuilder 已做，这里再次确认）
                    if (!req.getType().isAssignableFrom(sourceNode.getPayloadType())) {
                        // 这个理论上不应该发生，因为 ChainBuilder 已经检查过了
                        throw new IllegalStateException(
                                String.format("[%s] DAG '%s': 内部错误 - 节点 '%s' 的输入需求 %s (类型 %s) 与已验证的源节点 '%s' 的输出类型 (%s) 不兼容。",
                                        contextType.getSimpleName(), getDagName(), targetNodeName, req, req.getType().getSimpleName(),
                                        sourceNodeName, sourceNode.getPayloadType().getSimpleName()));
                    }
                } else if (!req.isOptional()) {
                    // 如果是必需输入，到这里还没找到源（显式映射没有，自动解析也没成功），则抛出异常
                    // （前面的逻辑应该已经覆盖了所有必需输入找不到源的情况，这里是最后防线）
                    throw new IllegalStateException(
                            String.format("[%s] DAG '%s': 节点 '%s' 的必需输入需求 %s 在验证结束时仍未找到有效的源映射。",
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
            // 抛出异常而不是警告，因为未初始化的顺序是无效的
            throw new IllegalStateException(String.format("[%s] DAG '%s' 尚未初始化，无法获取执行顺序。", contextType.getSimpleName(), getDagName()));
        }
        return executionOrder; // 返回的是不可修改列表
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
        Set<String> registeredNames = new HashSet<>(); // 用于检测重复名称

        for (DagNode<C, ?, ?> node : nodes) {
            String nodeName = node.getName();
            Class<?> payloadType = node.getPayloadType();
            Class<?> eventType = node.getEventType();

            // 基础验证
            if (nodeName == null || nodeName.trim().isEmpty()) {
                throw new IllegalStateException(String.format("[%s] DAG '%s': 检测到未命名节点 (实现类: %s)。节点必须有名称。",
                        contextType.getSimpleName(), getDagName(), node.getClass().getName()));
            }
            if (payloadType == null) {
                throw new IllegalStateException(String.format("[%s] DAG '%s': 节点 '%s' (实现类: %s) 未提供有效的 Payload 类型 (getPayloadType() 返回 null)。",
                        contextType.getSimpleName(), getDagName(), nodeName, node.getClass().getName()));
            }
            if (eventType == null) {
                throw new IllegalStateException(String.format("[%s] DAG '%s': 节点 '%s' (实现类: %s) 未提供有效的 Event 类型 (getEventType() 返回 null)。",
                        contextType.getSimpleName(), getDagName(), nodeName, node.getClass().getName()));
            }

            // 检查名称冲突
            if (!registeredNames.add(nodeName)) { // Set.add 返回 false 如果元素已存在
                DagNode<C, ?, ?> existingNode = nodesByName.get(nodeName); // 获取已存在的节点信息
                String existingNodeClass = (existingNode != null) ? existingNode.getClass().getName() : "未知";
                throw new IllegalStateException(String.format(
                        "[%s] DAG '%s': 节点名称冲突！名称 '%s' 已被节点 (类: %s) 使用，不能再被节点 (类: %s) 使用。节点名称在 DAG 中必须唯一。",
                        contextType.getSimpleName(), getDagName(), nodeName,
                        existingNodeClass,
                        node.getClass().getName()
                ));
            }

            nodesByName.put(nodeName, node);

            log.info("[{}] DAG '{}': 已注册节点: '{}' (Payload: {}, Event: {}, Impl: {})",
                    contextType.getSimpleName(), getDagName(), nodeName,
                    payloadType.getSimpleName(), eventType.getSimpleName(),
                    node.getClass().getSimpleName());
        }
        log.info("[{}] DAG '{}' 节点注册完成，共 {} 个唯一名称的节点", contextType.getSimpleName(), getDagName(), nodesByName.size());
    }

    // printDagStructure 和 generateDotGraph 基本不变，但可以更新以更好地区分执行依赖和输入映射
    private void printDagStructure() {
        if (!log.isInfoEnabled() || !initialized || nodesByName.isEmpty()) return; // 确保已初始化

        log.info("[{}] DAG '{}' 结构表示 (基于执行依赖和输入映射):", contextType.getSimpleName(), getDagName());
        StringBuilder builder = new StringBuilder("\n");
        builder.append(String.format("%-35s | %-30s | %-50s | %-50s | %-60s\n",
                "节点 (Payload, Event)", "实现类", "执行前驱", "执行后继", "输入映射 (需求 -> 配置的源节点)"));
        StringBuilder divider = new StringBuilder();
        for (int i = 0; i < 230; i++) divider.append("-");
        builder.append(divider).append("\n");

        // 使用执行顺序迭代，如果可用
        List<String> nodesToIterate = this.executionOrder;
        if (nodesToIterate.isEmpty()) { // 如果执行顺序为空（例如初始化失败），则按名称排序
            nodesToIterate = new ArrayList<>(nodesByName.keySet());
            Collections.sort(nodesToIterate);
        }


        if (nodesToIterate.isEmpty()) {
            builder.append("  (DAG 为空或未成功初始化)\n");
        } else {
            for (String nodeName : nodesToIterate) {
                DagNode<C, ?, ?> node = nodesByName.get(nodeName);
                if (node == null) continue;

                String nameAndTypes = String.format("%s (%s, %s)", nodeName,
                        node.getPayloadType().getSimpleName(), node.getEventType().getSimpleName());
                String implClass = node.getClass().getSimpleName();
                String predecessorsStr = formatNodeSet(executionPredecessors.getOrDefault(nodeName, Collections.emptySet()));
                String successorsStr = formatNodeSet(executionSuccessors.getOrDefault(nodeName, Collections.emptySet()));

                // 格式化输入映射 (显示配置的源)
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
            builder.append("\n计算出的执行顺序 (拓扑排序):\n");
            builder.append(String.join(" -> ", executionOrder));
        } else if (!nodesByName.isEmpty()) {
            builder.append("\n(无有效执行顺序 - DAG 未成功初始化)\n");
        }

        log.info(builder.toString());
    }

    private void generateDotGraph() {
        if (!log.isInfoEnabled() || !initialized || nodesByName.isEmpty()) return; // 确保已初始化

        StringBuilder dotGraph = new StringBuilder();
        String graphName = String.format("DAG_%s_%s", contextType.getSimpleName(), getDagName().replaceAll("\\W+", "_"));
        dotGraph.append(String.format("digraph %s {\n", graphName));
        dotGraph.append(String.format("  label=\"DAG Structure (%s - %s) - Execution (solid) & Input Mapping (dashed)\";\n", contextType.getSimpleName(), getDagName()));
        dotGraph.append("  labelloc=top;\n");
        dotGraph.append("  fontsize=16;\n");
        dotGraph.append("  rankdir=LR; // Left to Right layout\n");
        dotGraph.append("  node [shape=record, style=\"rounded,filled\", fillcolor=\"lightblue\", fontname=\"Arial\", fontsize=10];\n");
        dotGraph.append("  edge [fontname=\"Arial\", fontsize=9];\n\n");

        // 节点定义 (包含输入需求)
        for (String nodeName : nodesByName.keySet()) {
            DagNode<C, ?, ?> node = nodesByName.get(nodeName);
            if (node == null) continue;
            List<InputRequirement<?>> inputs = node.getInputRequirements();
            String inputLabel = inputs.isEmpty() ? "(none)" : inputs.stream()
                    .map(InputRequirement::toString)
                    .collect(Collectors.joining("\\n")); // Use \\n for newline in DOT label

            // 使用 HTML-like label 以获得更好的格式控制
            String label = String.format("< <TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">" +
                            "<TR><TD COLSPAN=\"2\" BGCOLOR=\"lightblue\">%s</TD></TR>" +
                            "<TR><TD>Payload</TD><TD>%s</TD></TR>" +
                            "<TR><TD>Event</TD><TD>%s</TD></TR>" +
                            "<TR><TD>Inputs</TD><TD ALIGN=\"LEFT\">%s</TD></TR>" + // ALIGN="LEFT" for input list
                            "</TABLE> >",
                    nodeName,
                    node.getPayloadType().getSimpleName(),
                    node.getEventType().getSimpleName(),
                    inputLabel.replace("\n", "<BR/>")); // Replace newline with HTML break
            dotGraph.append(String.format("  \"%s\" [shape=plain, label=%s];\n", nodeName, label)); // Use shape=plain for HTML labels
        }
        dotGraph.append("\n");

        // 执行依赖边 (实线, 黑色)
        dotGraph.append("  // Execution Edges (Solid, Black)\n");
        for (Map.Entry<String, Set<String>> entry : executionSuccessors.entrySet()) { // Use successors map
            String source = entry.getKey();
            for (String target : entry.getValue()) {
                dotGraph.append(String.format("  \"%s\" -> \"%s\" [style=solid, color=black, arrowhead=normal];\n", source, target));
            }
        }
        dotGraph.append("\n");

        // 输入映射边 (虚线, 蓝色, 带标签)
        dotGraph.append("  // Input Mapping Edges (Dashed, Blue)\n");
        for (Map.Entry<String, Map<InputRequirement<?>, String>> targetEntry : inputMappings.entrySet()) {
            String target = targetEntry.getKey();
            for (Map.Entry<InputRequirement<?>, String> mappingEntry : targetEntry.getValue().entrySet()) {
                InputRequirement<?> req = mappingEntry.getKey();
                String source = mappingEntry.getValue();
                // 确保源和目标都存在
                if (nodesByName.containsKey(source) && nodesByName.containsKey(target)) {
                    String edgeLabel = String.format("req: %s\\n(type: %s%s)",
                            req.getQualifier().map(q -> "'" + q + "'").orElse("(default)"),
                            req.getType().getSimpleName(),
                            req.isOptional() ? ", opt" : "");
                    // 使用不同的箭头或约束来区分数据流？暂时只用虚线和颜色
                    dotGraph.append(String.format("  \"%s\" -> \"%s\" [style=dashed, color=blue, label=\"%s\", constraint=false, arrowhead=open];\n",
                            source, target, edgeLabel)); // constraint=false 避免影响布局
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

}
