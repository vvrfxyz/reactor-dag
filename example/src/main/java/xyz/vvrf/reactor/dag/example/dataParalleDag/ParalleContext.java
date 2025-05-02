// [文件名称]: ParalleContext.java
package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.Data;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap; // 引入 ConcurrentHashMap

/**
 * reactor-dag
 * 并行 DAG 示例的上下文对象。
 * 添加了 nodeResults Map 用于存储节点执行结果，供下游节点读取。
 *
 * @author ruifeng.wen (重构)
 * @date (当前日期)
 */
@Data
public class ParalleContext {
    private long startTime = System.currentTimeMillis();

    /**
     * 存储节点执行结果或状态的 Map。
     * Key: 节点名称 (String)
     * Value: 节点的执行结果描述或状态 (String)
     * 使用 ConcurrentHashMap 保证线程安全，因为并行节点会并发写入。
     */
    private final Map<String, String> nodeResults = new ConcurrentHashMap<>();

    // 可以根据需要添加其他上下文属性
}
