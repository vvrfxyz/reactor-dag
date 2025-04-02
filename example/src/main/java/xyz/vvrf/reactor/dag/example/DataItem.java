package xyz.vvrf.reactor.dag.example;

/**
 * reactor-dag-example
 *
 * @author ruifeng.wen
 * @date 2025/4/2
 */

import lombok.Data;

/**
 * 数据项
 */
@Data
class DataItem {
    private String id;
    private String value;
    private boolean valid;
    private String enrichedInfo;
}
