// file: core/ErrorHandlingStrategy.java
package xyz.vvrf.reactor.dag.core;

/**
 * 定义 DAG 执行过程中的错误处理策略。
 *
 * @author ruifeng.wen (Refactored)
 */
public enum ErrorHandlingStrategy {
    /**
     * 快速失败：任何节点执行失败，整个 DAG 执行立即停止并报告失败。
     * 这是默认策略。
     */
    FAIL_FAST,

    /**
     * 继续执行：即使某个节点失败，也会尝试执行图中所有其他可执行的节点。
     * DAG 的最终状态取决于是否有任何节点失败。 (此策略的实现会更复杂)
     * (注意: 当前 StandardDagEngine 实现仅支持 FAIL_FAST)
     */
    CONTINUE_ON_FAILURE // 暂未完全实现
}
