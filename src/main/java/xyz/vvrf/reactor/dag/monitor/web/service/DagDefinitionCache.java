package xyz.vvrf.reactor.dag.monitor.web.service;

import org.springframework.stereotype.Service;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DagDefinitionCache {
    // Key: dagName, Value: DOT string
    private final Map<String, String> dotCache = new ConcurrentHashMap<>();
    // Key: dagName, Value: DagDefinition (用于其他可能的元数据访问)
    private final Map<String, DagDefinition<?>> definitionCache = new ConcurrentHashMap<>();
    // 修改：Key: dagName, Value: Cytoscape.js JSON string
    private final Map<String, String> cytoscapeJsCache = new ConcurrentHashMap<>();


    /**
     * 缓存 DAG 定义、DOT 描述以及 Cytoscape.js JSON 描述。
     * 这应该在 DagDefinition 构建完成后被调用。
     *
     * @param dagDefinition     DAG 定义对象
     * @param dotRepresentation DAG 的 DOT 字符串描述
     * @param cytoscapeJsJsonRepresentation DAG 的 Cytoscape.js JSON 字符串描述
     */
    public void cacheDag(DagDefinition<?> dagDefinition, String dotRepresentation, String cytoscapeJsJsonRepresentation) {
        if (dagDefinition == null || dagDefinition.getDagName() == null) {
            // 可以添加日志记录
            return;
        }
        String dagName = dagDefinition.getDagName();
        definitionCache.put(dagName, dagDefinition);

        if (dotRepresentation != null) {
            dotCache.put(dagName, dotRepresentation);
        }
        if (cytoscapeJsJsonRepresentation != null) {
            cytoscapeJsCache.put(dagName, cytoscapeJsJsonRepresentation);
        }
    }

    // 重载 cacheDag 以保持向后兼容性（如果外部只提供DOT）
    public void cacheDag(DagDefinition<?> dagDefinition, String dotRepresentation) {
        this.cacheDag(dagDefinition, dotRepresentation, null);
    }


    public Optional<String> getDotRepresentation(String dagName) {
        return Optional.ofNullable(dotCache.get(dagName));
    }

    public Optional<DagDefinition<?>> getDagDefinition(String dagName) {
        return Optional.ofNullable(definitionCache.get(dagName));
    }

    // 修改方法
    public Optional<String> getCytoscapeJsRepresentation(String dagName) {
        return Optional.ofNullable(cytoscapeJsCache.get(dagName));
    }

    public void clearCache() {
        dotCache.clear();
        definitionCache.clear();
        cytoscapeJsCache.clear(); // 清理新缓存
    }
}
