// file: core/NodeResult.java
package xyz.vvrf.reactor.dag.core;

import lombok.Getter;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.Optional;

/**
 * 节点执行结果 - 包含上下文、可选的 Payload、事件流和可能的错误。
 * 事件流类型为 Flux<Event<?>>，允许包含不同数据类型的事件。
 *
 * @param <C> 上下文类型 (Context Type)
 * @param <P> 结果 Payload 类型 (Payload Type)
 * @author ruifeng.wen (Refactored)
 */
public final class NodeResult<C, P> {

    public enum NodeStatus {
        /** 成功完成 */
        SUCCESS,
        /** 执行失败 */
        FAILURE,
        /** 因条件不满足或上游失败（取决于策略）而被跳过 */
        SKIPPED
    }

    @Getter
    private final C context;

    @Getter
    private final Optional<P> payload;

    @Getter
    private final Flux<Event<?>> events; // 事件流包含任意类型的事件

    private final Throwable error;

    @Getter
    private final Class<P> payloadType;

    @Getter
    private final NodeStatus status;

    /**
     * 私有构造函数，请使用静态工厂方法创建实例。
     */
    private NodeResult(C context, Optional<P> payload, Flux<Event<?>> events, Throwable error, Class<P> payloadType, NodeStatus status) {
        this.context = context; // Context 可以为 null 吗？假设可以
        this.payload = Objects.requireNonNull(payload, "Payload Optional 不能为 null");
        this.events = (events != null) ? events : Flux.empty(); // 确保 events 不为 null
        this.error = error;
        this.payloadType = Objects.requireNonNull(payloadType, "Payload 类型不能为空");
        this.status = Objects.requireNonNull(status, "NodeStatus 不能为空");

        if (status == NodeStatus.FAILURE && error == null) {
            throw new IllegalArgumentException("FAILURE status requires a non-null error.");
        }
        if (status != NodeStatus.FAILURE && error != null) {
            throw new IllegalArgumentException("Non-FAILURE status cannot have an error.");
        }
        if (status == NodeStatus.SKIPPED && payload.isPresent()) {
            // Skipped 节点理论上不应有 payload，但允许以防特殊场景，加个警告
            System.err.printf("Warning: SKIPPED NodeResult created with a present payload for type %s.%n", payloadType.getSimpleName());
        }
    }

    // --- 静态工厂方法 ---

    /**
     * 创建一个成功的节点结果，只包含 Payload。
     *
     * @param context     上下文对象
     * @param payload     节点产出的 Payload 数据 (可以为 null)
     * @param payloadType 结果的 Payload 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, P> NodeResult<C, P> success(C context, P payload, Class<P> payloadType) {
        return new NodeResult<>(context, Optional.ofNullable(payload), Flux.empty(), null, payloadType, NodeStatus.SUCCESS);
    }

    /**
     * 创建一个成功的节点结果，不包含 Payload。
     *
     * @param context     上下文对象
     * @param payloadType 结果的 Payload 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, P> NodeResult<C, P> success(C context, Class<P> payloadType) {
        return new NodeResult<>(context, Optional.empty(), Flux.empty(), null, payloadType, NodeStatus.SUCCESS);
    }

    /**
     * 创建一个成功的节点结果，只包含事件流。
     *
     * @param context     上下文对象
     * @param events      节点产生的事件流 (不能为空, 可以是 Flux.empty())
     * @param payloadType 结果的 Payload 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, P> NodeResult<C, P> success(C context, Flux<Event<?>> events, Class<P> payloadType) {
        Objects.requireNonNull(events, "事件流不能为空");
        return new NodeResult<>(context, Optional.empty(), events, null, payloadType, NodeStatus.SUCCESS);
    }

    /**
     * 创建一个成功的节点结果，同时包含 Payload 和事件流。
     *
     * @param context     上下文对象
     * @param payload     节点产出的 Payload 数据 (可以为 null)
     * @param events      节点产生的事件流 (不能为空, 可以是 Flux.empty())
     * @param payloadType 结果的 Payload 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, P> NodeResult<C, P> success(C context, P payload, Flux<Event<?>> events, Class<P> payloadType) {
        Objects.requireNonNull(events, "事件流不能为空");
        return new NodeResult<>(context, Optional.ofNullable(payload), events, null, payloadType, NodeStatus.SUCCESS);
    }

    /**
     * 创建一个失败的节点结果。
     *
     * @param context     上下文对象
     * @param error       执行过程中发生的错误 (不能为空)
     * @param payloadType 结果的 Payload 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, P> NodeResult<C, P> failure(C context, Throwable error, Class<P> payloadType) {
        Objects.requireNonNull(error, "错误对象不能为空");
        return new NodeResult<>(context, Optional.empty(), Flux.empty(), error, payloadType, NodeStatus.FAILURE);
    }

    /**
     * 创建一个表示节点被跳过的结果。
     *
     * @param context     上下文对象
     * @param payloadType 结果的 Payload 类型 Class 对象
     * @return NodeResult 实例，状态为 SKIPPED
     */
    public static <C, P> NodeResult<C, P> skipped(C context, Class<P> payloadType) {
        return new NodeResult<>(context, Optional.empty(), Flux.empty(), null, payloadType, NodeStatus.SKIPPED);
    }

    // --- 实例方法 ---

    /**
     * 获取执行过程中的错误。
     *
     * @return 包含错误的 Optional，如果没有错误则为空
     */
    public Optional<Throwable> getError() {
        return Optional.ofNullable(error);
    }

    /**
     * 检查执行是否成功。
     * @return 如果状态是 SUCCESS 则返回 true
     */
    public boolean isSuccess() {
        return this.status == NodeStatus.SUCCESS;
    }

    /**
     * 检查执行是否失败。
     * @return 如果状态是 FAILURE 则返回 true
     */
    public boolean isFailure() {
        return this.status == NodeStatus.FAILURE;
    }

    /**
     * 检查节点是否被跳过。
     * @return 如果状态是 SKIPPED 则返回 true
     */
    public boolean isSkipped() {
        return this.status == NodeStatus.SKIPPED;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeResult<?, ?> that = (NodeResult<?, ?>) o;
        // 注意：不比较 events (Flux) 和 context
        return Objects.equals(payload, that.payload) &&
                Objects.equals(error, that.error) &&
                Objects.equals(payloadType, that.payloadType) &&
                status == that.status;
    }

    @Override
    public int hashCode() {
        // 注意：不包含 events 和 context
        return Objects.hash(payload, error, payloadType, status);
    }

    @Override
    public String toString() {
        return "NodeResult{" +
                "status=" + status +
                ", payloadType=" + payloadType.getSimpleName() +
                ", payload=" + payload.map(p -> p.getClass().getSimpleName()).orElse("<empty>") +
                ", hasEvents=" + (events != Flux.<Event<?>>empty()) + // 简化判断
                ", error=" + error +
                '}';
    }
}
