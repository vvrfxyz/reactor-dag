package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.Data;

/**
 * DataParallel DAG 使用的上下文对象。
 * 可以包含 DAG 执行期间需要在节点间共享的数据或状态。
 */
@Data
public class ParalleContext {
    // 示例：可以添加一个初始输入值
    private String initialInput;

    // 示例：可以添加一个用于收集结果的字段（如果不在节点 Payload 中传递）
    // private Map<String, Object> results = new ConcurrentHashMap<>();

    public ParalleContext() {
        // 默认构造函数
    }

    public ParalleContext(String initialInput) {
        this.initialInput = initialInput;
    }
}
