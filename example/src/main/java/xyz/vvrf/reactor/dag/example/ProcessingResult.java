package xyz.vvrf.reactor.dag.example;

/**
 * reactor-dag-example
 *
 * @author ruifeng.wen
 * @date 2025/4/2
 */

import lombok.Data;

/**
 * 处理结果
 */
@Data
class ProcessingResult {
    private int totalItems;
    private int validItems;
    private int transformedItems;
    private int enrichedItems;
    private long processingTimeMs;
}