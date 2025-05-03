// file: util/GraphUtils.java
package xyz.vvrf.reactor.dag.util;

import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.builder.DagDefinitionBuilder.NodeBuildInfo; // 引用内部类

import java.util.*;
import java.util.stream.Collectors;

/**
 * 提供 DAG 图结构验证、循环检测和拓扑排序的工具方法。
 *
 * @author ruifeng.wen (Refactored)
 */
@Slf4j
public final class GraphUtils {

    private GraphUtils() { // 防止实例化
    }

    /**
     * 验证 DAG 图结构的完整性和类型兼容性。
     * - 检查所有连接的节点是否存在。
     * - 检查连接的类型是否兼容 (上游输出类型 assignable to 下游输入槽类型)。
     * - 检查所有声明了输入需求的节点是否有对应的输入连接。
     *
     * @param nodesInGraph 图中的节点信息 (InstanceName -> NodeBuildInfo)
     * @param wiring       图中的连接信息 (Downstream -> Map<InputSlot, Upstream>)
     * @param dagName      DAG 名称，用于日志
     * @throws IllegalStateException 如果验证失败
     */
    public static <C> void validateGraphStructure(
            Map<String, NodeBuildInfo<C>> nodesInGraph,
            Map<String, Map<String, String>> wiring,
            String dagName) throws IllegalStateException {

        log.debug("DAG '{}': Starting graph structure validation...", dagName);

        // 检查连接的有效性
        for (Map.Entry<String, Map<String, String>> downstreamEntry : wiring.entrySet()) {
            String downstreamName = downstreamEntry.getKey();
            Map<String, String> connections = downstreamEntry.getValue();

            NodeBuildInfo<C> downstreamInfo = nodesInGraph.get(downstreamName);
            if (downstreamInfo == null) {
                // 这个理论上不应该发生，因为 builder 在 wire 时检查了
                throw new IllegalStateException(String.format("DAG '%s': Wiring defined for non-existent downstream node '%s'.", dagName, downstreamName));
            }

            for (Map.Entry<String, String> connection : connections.entrySet()) {
                String inputSlot = connection.getKey();
                String upstreamName = connection.getValue();

                NodeBuildInfo<C> upstreamInfo = nodesInGraph.get(upstreamName);
                if (upstreamInfo == null) {
                    throw new IllegalStateException(String.format("DAG '%s': Node '%s' is wired to non-existent upstream node '%s' for input slot '%s'.",
                            dagName, downstreamName, upstreamName, inputSlot));
                }

                // 再次检查类型兼容性 (builder 也检查过，这里是最终确认)
                Class<?> requiredType = downstreamInfo.getInputRequirements().get(inputSlot);
                if (requiredType == null) {
                    // 这个理论上不应该发生
                    throw new IllegalStateException(String.format("DAG '%s': Node '%s' has wiring for undeclared input slot '%s'.",
                            dagName, downstreamName, inputSlot));
                }
                Class<?> providedType = upstreamInfo.getOutputType();
                if (!requiredType.isAssignableFrom(providedType)) {
                    throw new IllegalStateException(String.format("DAG '%s': Type mismatch remains for input '%s' of node '%s'. Required: %s, Provided by '%s': %s",
                            dagName, inputSlot, downstreamName, requiredType.getSimpleName(),
                            upstreamName, providedType.getSimpleName()));
                }
            }
        }

        // 检查所有有输入需求的节点是否都有连接
        for (Map.Entry<String, NodeBuildInfo<C>> nodeEntry : nodesInGraph.entrySet()) {
            String nodeName = nodeEntry.getKey();
            NodeBuildInfo<C> nodeInfo = nodeEntry.getValue();
            Map<String, Class<?>> requirements = nodeInfo.getInputRequirements();
            Map<String, String> actualWiring = wiring.getOrDefault(nodeName, Collections.emptyMap());

            for (String requiredSlot : requirements.keySet()) {
                if (!actualWiring.containsKey(requiredSlot)) {
                    throw new IllegalStateException(String.format("DAG '%s': Node '%s' requires input for slot '%s' (type %s), but it is not wired.",
                            dagName, nodeName, requiredSlot, requirements.get(requiredSlot).getSimpleName()));
                }
            }
            // (可选) 检查是否有未使用的连接定义 (wiring 中有，但节点不声明该 input slot)
            for (String wiredSlot : actualWiring.keySet()) {
                if (!requirements.containsKey(wiredSlot)) {
                    log.warn("DAG '{}': Node '{}' has wiring defined for input slot '{}', but the node implementation does not declare this requirement. This wiring will be ignored.",
                            dagName, nodeName, wiredSlot);
                    // 注意：这里可以选择抛出异常，或者仅警告。当前选择警告。
                }
            }
        }

        log.debug("DAG '{}': Graph structure validation passed.", dagName);
    }

    /**
     * 使用深度优先搜索 (DFS) 检测 DAG 中是否存在循环。
     *
     * @param allNodeNames 图中所有节点的名称集合
     * @param wiring       图的连接关系 (Downstream -> Map<InputSlot, Upstream>)
     * @param dagName      DAG 名称，用于日志
     * @throws IllegalStateException 如果检测到循环
     */
    public static void detectCycles(
            Set<String> allNodeNames,
            Map<String, Map<String, String>> wiring,
            String dagName) throws IllegalStateException {

        log.debug("DAG '{}': Starting cycle detection...", dagName);
        Set<String> visited = new HashSet<>(); // 完全访问过的节点
        Set<String> visiting = new HashSet<>(); // 当前递归路径上的节点

        for (String nodeName : allNodeNames) {
            if (!visited.contains(nodeName)) {
                if (hasCycleDFS(nodeName, visited, visiting, wiring, dagName)) {
                    // 异常已在 hasCycleDFS 内部抛出，这里只是为了逻辑完整性
                    return;
                }
            }
        }
        log.debug("DAG '{}': No cycles detected.", dagName);
    }

    // DFS 辅助方法
    private static boolean hasCycleDFS(
            String nodeName,
            Set<String> visited,
            Set<String> visiting,
            Map<String, Map<String, String>> wiring,
            String dagName) {

        visited.add(nodeName);
        visiting.add(nodeName);

        // 找到所有依赖当前节点 (nodeName) 的下游节点
        for (Map.Entry<String, Map<String, String>> downstreamEntry : wiring.entrySet()) {
            String downstreamNode = downstreamEntry.getKey();
            Map<String, String> connections = downstreamEntry.getValue();
            // 检查下游节点的连接是否包含当前节点作为上游
            if (connections.containsValue(nodeName)) {
                // downstreamNode 依赖于 nodeName
                if (visiting.contains(downstreamNode)) {
                    // 发现循环！
                    throw new IllegalStateException(String.format("DAG '%s': Cycle detected! Path involves edge from '%s' to '%s'.",
                            dagName, nodeName, downstreamNode));
                }
                if (!visited.contains(downstreamNode)) {
                    // 递归检查下游节点
                    if (hasCycleDFS(downstreamNode, visited, visiting, wiring, dagName)) {
                        return true; // 循环已找到并抛出异常
                    }
                }
                // 如果下游节点已访问过且不在当前路径上，则安全
            }
        }


        visiting.remove(nodeName); // 回溯，将节点移出当前路径
        return false; // 从此节点出发未找到循环
    }


    /**
     * 使用 Kahn 算法计算 DAG 的拓扑排序。
     *
     * @param allNodeNames 图中所有节点的名称集合
     * @param wiring       图的连接关系 (Downstream -> Map<InputSlot, Upstream>)
     * @param dagName      DAG 名称，用于日志
     * @return 按拓扑顺序排列的节点名称列表
     * @throws IllegalStateException 如果图包含循环 (虽然 cycle detection 应该先捕捉到) 或图不连通导致排序不完整
     */
    public static List<String> topologicalSort(
            Set<String> allNodeNames,
            Map<String, Map<String, String>> wiring,
            String dagName) throws IllegalStateException {

        log.debug("DAG '{}': Starting topological sort...", dagName);
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> adj = new HashMap<>(); // 邻接表: Upstream -> List<Downstream>

        // 初始化入度和邻接表
        for (String nodeName : allNodeNames) {
            inDegree.put(nodeName, 0);
            adj.put(nodeName, new ArrayList<>());
        }

        // 构建图和计算入度
        for (Map.Entry<String, Map<String, String>> downstreamEntry : wiring.entrySet()) {
            String downstreamNode = downstreamEntry.getKey();
            Map<String, String> connections = downstreamEntry.getValue();
            for (String upstreamNode : connections.values()) {
                // 添加边: upstream -> downstream
                if (adj.containsKey(upstreamNode)) { // 防御性检查
                    adj.get(upstreamNode).add(downstreamNode);
                    inDegree.put(downstreamNode, inDegree.getOrDefault(downstreamNode, 0) + 1);
                } else {
                    // 理论上不应发生，因为 allNodeNames 包含了所有节点
                    throw new IllegalStateException(String.format("DAG '%s': Inconsistent state during topological sort. Upstream node '%s' not found in node set.", dagName, upstreamNode));
                }
            }
        }

        // 将所有入度为 0 的节点加入队列
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

            // 对于 u 的每个邻居 v
            for (String v : adj.getOrDefault(u, Collections.emptyList())) {
                inDegree.put(v, inDegree.get(v) - 1);
                if (inDegree.get(v) == 0) {
                    queue.offer(v);
                }
            }
        }

        // 检查排序结果是否包含所有节点
        if (sortedOrder.size() != allNodeNames.size()) {
            Set<String> remainingNodes = new HashSet<>(allNodeNames);
            remainingNodes.removeAll(sortedOrder);
            throw new IllegalStateException(String.format("DAG '%s': Topological sort failed. Graph might contain a cycle or be disconnected. Unsorted nodes: %s",
                    dagName, remainingNodes));
        }

        log.debug("DAG '{}': Topological sort successful.", dagName);
        return Collections.unmodifiableList(sortedOrder); // 返回不可变列表
    }
}
