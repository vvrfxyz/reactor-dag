package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.Data;

/**
 * reactor-dag
 * 并行 DAG 示例的上下文对象。
 * (此文件无需修改)
 *
 * @author ruifeng.wen
 * @date 4/19/25
 */
@Data
public class ParalleContext {
    // 可以根据需要添加上下文属性
    private long startTime = System.currentTimeMillis();
}
