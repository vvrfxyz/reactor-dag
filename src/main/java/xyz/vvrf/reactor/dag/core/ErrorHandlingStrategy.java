package xyz.vvrf.reactor.dag.core;

/**
 * 定义 DAG 执行过程中的错误处理策略。
 *
 * @author Refactored (注释更新)
 */
public enum ErrorHandlingStrategy {
    /**
     * 快速失败：任何节点执行失败（返回 FAILURE 状态或抛出异常），整个 DAG 执行将尽快停止。
     * 未执行或正在执行的节点将被取消或跳过。DAG 最终结果为失败。
     * 这是默认策略。
     */
    FAIL_FAST,

    /**
     * 继续执行：即使某个节点失败，引擎也会尝试执行图中所有其他可执行的节点
     * （即其依赖已满足且未失败，并且边条件满足）。
     * DAG 的最终状态取决于是否有任何节点失败。如果至少有一个节点失败，DAG 最终结果为失败，
     * 但会尽可能多地完成其他分支的执行。
     * (注意: 此策略的实现相对复杂，需要引擎正确处理失败节点的下游)
     */
    CONTINUE_ON_FAILURE
}
