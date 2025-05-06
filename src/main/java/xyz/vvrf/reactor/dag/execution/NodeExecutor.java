// file: execution/NodeExecutor.java
package xyz.vvrf.reactor.dag.execution;

import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.InputAccessor;
import xyz.vvrf.reactor.dag.core.NodeDefinition;
import xyz.vvrf.reactor.dag.core.NodeResult;

/**
 * 节点执行器接口 (更新后)。
 * 负责执行单个节点实例的逻辑。
 *
 * @param <C> 上下文类型
 * @author Refactored
 */
public interface NodeExecutor<C> {

    /**
     * 执行指定的节点实例。
     *
     * @param nodeDefinition 要执行的节点定义
     * @param context        当前上下文
     * @param inputAccessor  由引擎创建并传入的、包含激活输入数据的访问器
     * @param requestId      请求 ID
     * @return 包含节点执行结果的 Mono<NodeResult<C, ?>>
     */
    Mono<NodeResult<C, ?>> executeNode(
            NodeDefinition nodeDefinition, // 传入节点定义
            C context,
            InputAccessor<C> inputAccessor, // 传入准备好的 InputAccessor
            String requestId
    );
}
