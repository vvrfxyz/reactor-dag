// file: util/GraphUtils.java
package xyz.vvrf.reactor.dag.util;

import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.*;
import xyz.vvrf.reactor.dag.registry.NodeRegistry; // 需要注册表

import java.util.*;
import java.util.stream.Collectors;

/**
 * 提供 DAG 图结构验证、循环检测和拓扑排序的工具方法。
 * 现在基于 DagDefinition 数据结构操作。
 *
 * @author Refactored
 */
@Slf4j
public final class GraphUtils {

    private GraphUtils() {}

    /**
     * 验证 DAG 图结构的完整性和类型兼容性。
     *
     * @param nodeDefinitions 节点定义 Map
     * @param edgeDefinitions 边定义列表
     * @param nodeRegistry    节点注册表，用于获取元数据
     * @param dagName         DAG 名称
     * @throws IllegalStateException 如果验证失败
     */
    public static <C> void validateGraphStructure(
            Map<String, NodeDefinition> nodeDefinitions,
            List<EdgeDefinition<C>> edgeDefinitions,
            NodeRegistry<C> nodeRegistry,
            String dagName) throws IllegalStateException {

        log.debug("DAG '{}': Starting graph structure validation...", dagName);

        Set<String> nodeNames = nodeDefinitions.keySet();

        // 检查边的有效性
        for (EdgeDefinition<C> edge : edgeDefinitions) {
            String upName = edge.getUpstreamInstanceName();
            String downName = edge.getDownstreamInstanceName();
            String outSlotId = edge.getOutputSlotId();
            String inSlotId = edge.getInputSlotId();

            // 检查节点是否存在
            if (!nodeNames.contains(upName)) {
                throw new IllegalStateException(String.format("DAG '%s': Edge references non-existent upstream node '%s'.", dagName, upName));
            }
            if (!nodeNames.contains(downName)) {
                throw new IllegalStateException(String.format("DAG '%s': Edge references non-existent downstream node '%s'.", dagName, downName));
            }

            NodeDefinition upDef = nodeDefinitions.get(upName);
            NodeDefinition downDef = nodeDefinitions.get(downName);

            // 获取元数据
            NodeRegistry.NodeMetadata upMeta = nodeRegistry.getNodeMetadata(upDef.getNodeTypeId())
                    .orElseThrow(() -> new IllegalStateException(String.format("DAG '%s': Metadata not found in registry for upstream node type '%s' (Instance: '%s').", dagName, upDef.getNodeTypeId(), upName)));
            NodeRegistry.NodeMetadata downMeta = nodeRegistry.getNodeMetadata(downDef.getNodeTypeId())
                    .orElseThrow(() -> new IllegalStateException(String.format("DAG '%s': Metadata not found in registry for downstream node type '%s' (Instance: '%s').", dagName, downDef.getNodeTypeId(), downName)));

            // 检查插槽是否存在和类型匹配 (Builder 已检查，这里是最终确认)
            OutputSlot<?> outputSlot = upMeta.findOutputSlot(outSlotId)
                    .orElseThrow(() -> new IllegalStateException(String.format("DAG '%s': Output slot '%s' not found for node type '%s' (Instance: '%s').", dagName, outSlotId, upDef.getNodeTypeId(), upName)));
            InputSlot<?> inputSlot = downMeta.findInputSlot(inSlotId)
                    .orElseThrow(() -> new IllegalStateException(String.format("DAG '%s': Input slot '%s' not found for node type '%s' (Instance: '%s').", dagName, inSlotId, downDef.getNodeTypeId(), downName)));

            if (!inputSlot.getType().isAssignableFrom(outputSlot.getType())) {
                throw new IllegalStateException(String.format("DAG '%s': Type mismatch remains for edge %s[%s] -> %s[%s]. Required: %s, Provided: %s",
                        dagName, upName, outSlotId, downName, inSlotId, inputSlot.getType().getName(), outputSlot.getType().getName()));
            }
        }

        // 检查所有必需的输入槽是否都有至少一个入边连接
        Map<String, List<EdgeDefinition<C>>> incomingEdgesMap = edgeDefinitions.stream()
                .collect(Collectors.groupingBy(EdgeDefinition::getDownstreamInstanceName));

        for (Map.Entry<String, NodeDefinition> nodeEntry : nodeDefinitions.entrySet()) {
            String nodeName = nodeEntry.getKey();
            NodeDefinition nodeDef = nodeEntry.getValue();
            NodeRegistry.NodeMetadata meta = nodeRegistry.getNodeMetadata(nodeDef.getNodeTypeId()).get(); // 此时一定存在

            Set<String> requiredInputSlotIds = meta.getInputSlots().stream()
                    .filter(InputSlot::isRequired)
                    .map(InputSlot::getId)
                    .collect(Collectors.toSet());

            if (!requiredInputSlotIds.isEmpty()) {
                Set<String> connectedInputSlotIds = incomingEdgesMap.getOrDefault(nodeName, Collections.emptyList())
                        .stream()
                        .map(EdgeDefinition::getInputSlotId)
                        .collect(Collectors.toSet());

                requiredInputSlotIds.removeAll(connectedInputSlotIds); // 移除所有被连接的必需槽

                if (!requiredInputSlotIds.isEmpty()) {
                    throw new IllegalStateException(String.format("DAG '%s': Node '%s' (Type: '%s') has required input slots [%s] that are not connected by any incoming edge.",
                            dagName, nodeName, nodeDef.getNodeTypeId(), String.join(", ", requiredInputSlotIds)));
                }
            }
        }


        log.debug("DAG '{}': Graph structure validation passed.", dagName);
    }

    /**
     * 使用深度优先搜索 (DFS) 检测 DAG 中是否存在循环。
     *
     * @param allNodeNames    图中所有节点的名称集合
     * @param edgeDefinitions 图的边定义列表
     * @param dagName         DAG 名称
     * @throws IllegalStateException 如果检测到循环
     */
    public static <C> void detectCycles(
            Set<String> allNodeNames,
            List<EdgeDefinition<C>> edgeDefinitions,
            String dagName) throws IllegalStateException {

        log.debug("DAG '{}': Starting cycle detection...", dagName);
        Set<String> visited = new HashSet<>(); // 完全访问过的节点
        Set<String> visiting = new HashSet<>(); // 当前递归路径上的节点
        Map<String, List<String>> adj = buildAdjacencyList(edgeDefinitions); // 构建邻接表

        for (String nodeName : allNodeNames) {
            if (!visited.contains(nodeName)) {
                if (hasCycleDFS(nodeName, visited, visiting, adj, dagName)) {
                    // 异常已在内部抛出
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
            Map<String, List<String>> adj, // Upstream -> List<Downstream>
            String dagName) {

        visited.add(nodeName);
        visiting.add(nodeName);

        for (String neighbor : adj.getOrDefault(nodeName, Collections.emptyList())) {
            if (visiting.contains(neighbor)) {
                // 发现循环！
                throw new IllegalStateException(String.format("DAG '%s': Cycle detected! Path involves edge from '%s' to '%s'.",
                        dagName, nodeName, neighbor)); // 可能需要更复杂的路径追踪
            }
            if (!visited.contains(neighbor)) {
                if (hasCycleDFS(neighbor, visited, visiting, adj, dagName)) {
                    return true; // 循环已找到并抛出异常
                }
            }
        }

        visiting.remove(nodeName); // 回溯
        return false;
    }


    /**
     * 使用 Kahn 算法计算 DAG 的拓扑排序。
     *
     * @param allNodeNames    图中所有节点的名称集合
     * @param edgeDefinitions 图的边定义列表
     * @param dagName         DAG 名称
     * @return 按拓扑顺序排列的节点名称列表
     * @throws IllegalStateException 如果图包含循环或排序不完整
     */
    public static <C> List<String> topologicalSort(
            Set<String> allNodeNames,
            List<EdgeDefinition<C>> edgeDefinitions,
            String dagName) throws IllegalStateException {

        log.debug("DAG '{}': Starting topological sort...", dagName);
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> adj = buildAdjacencyList(edgeDefinitions); // Upstream -> List<Downstream>

        // 初始化入度
        for (String nodeName : allNodeNames) {
            inDegree.put(nodeName, 0);
        }

        // 计算入度
        for (List<String> neighbors : adj.values()) {
            for (String neighbor : neighbors) {
                inDegree.put(neighbor, inDegree.getOrDefault(neighbor, 0) + 1);
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

        // 检查排序结果
        if (sortedOrder.size() != allNodeNames.size()) {
            Set<String> remainingNodes = new HashSet<>(allNodeNames);
            remainingNodes.removeAll(sortedOrder);
            throw new IllegalStateException(String.format("DAG '%s': Topological sort failed. Graph might contain a cycle or be disconnected. Unsorted nodes: %s",
                    dagName, remainingNodes));
        }

        log.debug("DAG '{}': Topological sort successful.", dagName);
        return Collections.unmodifiableList(sortedOrder);
    }

    // 辅助方法：构建邻接表 (Upstream -> List<Downstream>)
    private static <C> Map<String, List<String>> buildAdjacencyList(List<EdgeDefinition<C>> edgeDefinitions) {
        Map<String, List<String>> adj = new HashMap<>();
        for (EdgeDefinition<C> edge : edgeDefinitions) {
            adj.computeIfAbsent(edge.getUpstreamInstanceName(), k -> new ArrayList<>())
                    .add(edge.getDownstreamInstanceName());
        }
        return adj;
    }
}
