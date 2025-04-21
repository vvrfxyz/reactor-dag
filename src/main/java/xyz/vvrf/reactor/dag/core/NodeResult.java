package xyz.vvrf.reactor.dag.core;

import lombok.Getter;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.Optional;

/**
 * 节点执行结果 - 包含上下文、可选的 Payload、事件流和可能的错误。
 *
 * @param <C> 上下文类型 (Context Type)
 * @param <P> 结果 Payload 类型 (Payload Type)
 * @param <T> 结果事件数据类型 (Event Data Type)
 * @author ruifeng.wen
 */
public final class NodeResult<C, P, T> {

    public enum NodeStatus {
        /** 尚未开始执行 (理论上 NodeResult 不应处于此状态) */
        PENDING,
        /** 正在执行中 (理论上 NodeResult 不应处于此状态) */
        RUNNING,
        /** 成功完成 */
        SUCCESS,
        /** 执行失败 */
        FAILURE,
        /** 因条件不满足而被跳过 */
        SKIPPED
    }

    @Getter
    private final C context;

    @Getter
    private final Optional<P> payload;

    @Getter
    private final Flux<Event<T>> events;

    private final Throwable error;

    @Getter
    private final Class<P> payloadType;

    @Getter
    private final Class<T> eventType;

    @Getter
    private final NodeStatus status;
    /**
     * 私有构造函数，请使用静态工厂方法创建实例。
     */
    private NodeResult(C context, Optional<P> payload, Flux<Event<T>> events, Throwable error, Class<P> payloadType, Class<T> eventType, NodeStatus status) {
        this.context = context;
        this.payload = Objects.requireNonNull(payload, "Payload Optional 不能为 null");
        this.events = (events != null) ? events : Flux.empty();
        this.error = error;
        this.payloadType = Objects.requireNonNull(payloadType, "Payload 类型不能为空");
        this.eventType = Objects.requireNonNull(eventType, "Event 类型不能为空");
        this.status = Objects.requireNonNull(status, "NodeStatus 不能为空");
    }

    /**
     * 创建一个成功的节点结果，只包含 Payload。
     * 类型通过 DagNode 推断。
     *
     * @param context 上下文对象
     * @param payload 节点产出的 Payload 数据 (可以为 null)
     * @param node    产生此结果的 DagNode 实例
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, P payload, DagNode<C, P, T> node) {
        Objects.requireNonNull(node, "DagNode 实例不能为空");
        return new NodeResult<>(context, Optional.ofNullable(payload), Flux.<Event<T>>empty(), null, node.getPayloadType(), node.getEventType(), NodeStatus.SUCCESS);
    }

    /**
     * 创建一个成功的节点结果，只包含事件流。
     * 类型通过 DagNode 推断。
     *
     * @param context 上下文对象
     * @param events  节点产生的事件流 (不能为空)
     * @param node    产生此结果的 DagNode 实例
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, Flux<Event<T>> events, DagNode<C, P, T> node) {
        Objects.requireNonNull(events, "事件流不能为空");
        Objects.requireNonNull(node, "DagNode 实例不能为空");
        return new NodeResult<>(context, Optional.empty(), events, null, node.getPayloadType(), node.getEventType(), NodeStatus.SUCCESS);
    }

    /**
     * 创建一个成功的节点结果，同时包含 Payload 和事件流。
     * 类型通过 DagNode 推断。
     *
     * @param context 上下文对象
     * @param payload 节点产出的 Payload 数据 (可以为 null)
     * @param events  节点产生的事件流 (不能为空)
     * @param node    产生此结果的 DagNode 实例
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, P payload, Flux<Event<T>> events, DagNode<C, P, T> node) {
        Objects.requireNonNull(events, "事件流不能为空");
        Objects.requireNonNull(node, "DagNode 实例不能为空");
        return new NodeResult<>(context, Optional.ofNullable(payload), events, null, node.getPayloadType(), node.getEventType(),NodeStatus.SUCCESS);
    }

    /**
     * 创建一个失败的节点结果。
     * 类型通过 DagNode 推断。
     *
     * @param context 上下文对象
     * @param error   执行过程中发生的错误 (不能为空)
     * @param node    尝试执行并失败的 DagNode 实例
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> failure(C context, Throwable error, DagNode<C, P, T> node) {
        Objects.requireNonNull(error, "错误对象不能为空");
        Objects.requireNonNull(node, "DagNode 实例不能为空");
        return new NodeResult<>(context, Optional.empty(), Flux.<Event<T>>empty(), error, node.getPayloadType(), node.getEventType(), NodeStatus.FAILURE);
    }

    /**
     * 创建一个成功的节点结果，只包含 Payload。
     * 需要显式提供 Payload 和 Event 类型。
     *
     * @param context     上下文对象
     * @param payload     节点产出的 Payload 数据 (可以为 null)
     * @param payloadType 结果的 Payload 类型 Class 对象
     * @param eventType   结果的 Event 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, P payload, Class<P> payloadType, Class<T> eventType) {
        return new NodeResult<>(context, Optional.ofNullable(payload), Flux.<Event<T>>empty(), null, payloadType, eventType, NodeStatus.SUCCESS);
    }

    /**
     * 创建一个成功的节点结果，只包含事件流。
     * 需要显式提供 Payload 和 Event 类型。
     *
     * @param context     上下文对象
     * @param events      节点产生的事件流 (不能为空)
     * @param payloadType 结果的 Payload 类型 Class 对象
     * @param eventType   结果的 Event 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, Flux<Event<T>> events, Class<P> payloadType, Class<T> eventType) {
        Objects.requireNonNull(events, "事件流不能为空");
        return new NodeResult<>(context, Optional.empty(), events, null, payloadType, eventType, NodeStatus.SUCCESS);
    }

    /**
     * 创建一个成功的节点结果，同时包含 Payload 和事件流。
     * 需要显式提供 Payload 和 Event 类型。
     *
     * @param context     上下文对象
     * @param payload     节点产出的 Payload 数据 (可以为 null)
     * @param events      节点产生的事件流 (不能为空)
     * @param payloadType 结果的 Payload 类型 Class 对象
     * @param eventType   结果的 Event 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, P payload, Flux<Event<T>> events, Class<P> payloadType, Class<T> eventType) {
        Objects.requireNonNull(events, "事件流不能为空");
        return new NodeResult<>(context, Optional.ofNullable(payload), events, null, payloadType, eventType, NodeStatus.SUCCESS);
    }

    /**
     * 创建一个失败的节点结果。
     * 需要显式提供 Payload 和 Event 类型。
     *
     * @param context     上下文对象
     * @param error       执行过程中发生的错误 (不能为空)
     * @param payloadType 结果的 Payload 类型 Class 对象
     * @param eventType   结果的 Event 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> failure(C context, Throwable error, Class<P> payloadType, Class<T> eventType) {
        Objects.requireNonNull(error, "错误对象不能为空");
        return new NodeResult<>(context, Optional.empty(), Flux.<Event<T>>empty(), error, payloadType, eventType, NodeStatus.FAILURE);
    }

    /**
     * 创建一个表示节点被跳过的结果。
     * 需要显式提供 Payload 和 Event 类型。
     *
     * @param context     上下文对象
     * @param payloadType 结果的 Payload 类型 Class 对象
     * @param eventType   结果的 Event 类型 Class 对象
     * @return NodeResult 实例，状态为 SKIPPED
     */
    public static <C, P, T> NodeResult<C, P, T> skipped(C context, Class<P> payloadType, Class<T> eventType) {
        return new NodeResult<>(context, Optional.empty(), Flux.<Event<T>>empty(), null, payloadType, eventType, NodeStatus.SKIPPED);
    }

    /**
     * 创建一个表示节点被跳过的结果。
     *
     * @param context 上下文对象
     * @param node    被跳过的 DagNode 实例
     * @return NodeResult 实例，状态为 SKIPPED
     */
    public static <C, P, T> NodeResult<C, P, T> skipped(C context, DagNode<C, P, T> node) {
        Objects.requireNonNull(node, "DagNode 实例不能为空");
        // 跳过的节点没有 Payload 和事件
        return new NodeResult<>(context, Optional.empty(), Flux.<Event<T>>empty(), null, node.getPayloadType(), node.getEventType(), NodeStatus.SKIPPED); // 设置 SKIPPED
    }

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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeResult<?, ?, ?> that = (NodeResult<?, ?, ?>) o;
        // 注意：不比较 events (Flux)
        return Objects.equals(context, that.context) &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(error, that.error) &&
                Objects.equals(payloadType, that.payloadType) &&
                Objects.equals(eventType, that.eventType) &&
                status == that.status; // 比较 status
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, payload, error, payloadType, eventType, status);
    }

    @Override
    public String toString() {
        return "NodeResult{" +
                "status=" + status + // 显示 status
                ", context=" + context +
                ", payload=" + payload.map(Object::toString).orElse("<empty>") +
                ", payloadType=" + payloadType.getSimpleName() +
                ", eventType=" + eventType.getSimpleName() +
                ", hasEvents=" + (events != Flux.<Event<T>>empty() && status == NodeStatus.SUCCESS) +
                ", error=" + error +
                '}';
    }
}
