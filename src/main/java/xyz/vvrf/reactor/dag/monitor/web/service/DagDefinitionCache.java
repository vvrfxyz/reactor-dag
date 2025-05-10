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

    /**
     * 缓存 DAG 定义及其 DOT 描述。
     * 这应该在 DagDefinition 构建完成后被调用。
     *
     * @param dagDefinition     DAG 定义对象
     * @param dotRepresentation DAG 的 DOT 字符串描述
     */
    public void cacheDag(DagDefinition<?> dagDefinition, String dotRepresentation) {
        if (dagDefinition == null || dagDefinition.getDagName() == null || dotRepresentation == null) {
            // 可以添加日志记录
            return;
        }
        String dagName = dagDefinition.getDagName();
        dotCache.put(dagName, dotRepresentation);
        definitionCache.put(dagName, dagDefinition);
    }

    public Optional<String> getDotRepresentation(String dagName) {
        return Optional.ofNullable(dotCache.get(dagName));
    }

    public Optional<DagDefinition<?>> getDagDefinition(String dagName) {
        return Optional.ofNullable(definitionCache.get(dagName));
    }

    public void clearCache() {
        dotCache.clear();
        definitionCache.clear();
    }
}
