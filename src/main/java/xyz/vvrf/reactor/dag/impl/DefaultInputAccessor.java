// file: impl/DefaultInputAccessor.java
package xyz.vvrf.reactor.dag.impl;

import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.InputAccessor;
import xyz.vvrf.reactor.dag.core.NodeResult;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * InputAccessor 的默认实现。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen (Refactored)
 */
public class DefaultInputAccessor<C> implements InputAccessor<C> {

    private final String currentNodeName;
    private final Map<String, String> upstreamWiring; // inputSlot -> upstreamInstanceName
    private final Map<String, NodeResult<C, ?>> allResults; // upstreamInstanceName -> NodeResult

    /**
     * 创建 DefaultInputAccessor 实例。
     *
     * @param currentNodeName 当前正在执行的节点实例的名称
     * @param dagDefinition   DAG 定义，用于获取连接关系
     * @param allResults      当前执行中所有已完成（或正在进行）的上游节点的结果 Map
     */
    public DefaultInputAccessor(String currentNodeName,
                                DagDefinition<C> dagDefinition,
                                Map<String, NodeResult<C, ?>> allResults) {
        this.currentNodeName = Objects.requireNonNull(currentNodeName, "Current node name cannot be null");
        Objects.requireNonNull(dagDefinition, "DAG Definition cannot be null");
        this.upstreamWiring = dagDefinition.getUpstreamWiring(currentNodeName); // 获取当前节点的连接
        this.allResults = Objects.requireNonNull(allResults, "Results map cannot be null");
    }

    private Optional<NodeResult<C, ?>> getUpstreamResult(String inputSlotName) {
        String upstreamNodeName = upstreamWiring.get(inputSlotName);
        if (upstreamNodeName == null) {
            // 输入槽未连接
            return Optional.empty();
        }
        return Optional.ofNullable(allResults.get(upstreamNodeName));
    }

    @Override
    public <InputP> Optional<InputP> getPayload(String inputSlotName, Class<InputP> expectedType) {
        Objects.requireNonNull(inputSlotName, "Input slot name cannot be null");
        Objects.requireNonNull(expectedType, "Expected payload type cannot be null");

        return getUpstreamResult(inputSlotName)
                .filter(NodeResult::isSuccess) // 必须是成功的结果
                .flatMap(NodeResult::getPayload) // 获取 Optional<Payload>
                .filter(expectedType::isInstance) // 检查类型
                .map(expectedType::cast); // 转换类型
    }

    @Override
    public boolean isInputAvailable(String inputSlotName) {
        return getUpstreamResult(inputSlotName)
                .filter(NodeResult::isSuccess)
                .flatMap(NodeResult::getPayload)
                .isPresent();
        // 或者更宽松的定义：只要上游成功就算可用？
        // return getUpstreamResult(inputSlotName).map(NodeResult::isSuccess).orElse(false);
    }

    @Override
    public boolean isInputFailed(String inputSlotName) {
        return getUpstreamResult(inputSlotName)
                .map(NodeResult::isFailure)
                .orElse(false); // 未连接或无结果不算失败
    }

    @Override
    public boolean isInputSkipped(String inputSlotName) {
        return getUpstreamResult(inputSlotName)
                .map(NodeResult::isSkipped)
                .orElse(false); // 未连接或无结果不算跳过
    }

    @Override
    public Optional<Throwable> getInputError(String inputSlotName) {
        return getUpstreamResult(inputSlotName)
                .filter(NodeResult::isFailure) // 只有失败的才有 Error
                .flatMap(NodeResult::getError);
    }
}
