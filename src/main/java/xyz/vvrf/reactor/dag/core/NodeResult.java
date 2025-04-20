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

    /**
     * 私有构造函数，请使用静态工厂方法创建实例。
     */
    private NodeResult(C context, Optional<P> payload, Flux<Event<T>> events, Throwable error, Class<P> payloadType, Class<T> eventType) {
        this.context = context;
        this.payload = Objects.requireNonNull(payload, "Payload Optional 不能为 null");
        this.events = (events != null) ? events : Flux.empty();
        this.error = error;
        this.payloadType = Objects.requireNonNull(payloadType, "Payload 类型不能为空");
        this.eventType = Objects.requireNonNull(eventType, "Event 类型不能为空");
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
        return new NodeResult<>(context, Optional.ofNullable(payload), Flux.<Event<T>>empty(), null, node.getPayloadType(), node.getEventType());
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
        return new NodeResult<>(context, Optional.empty(), events, null, node.getPayloadType(), node.getEventType());
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
        return new NodeResult<>(context, Optional.ofNullable(payload), events, null, node.getPayloadType(), node.getEventType());
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
        return new NodeResult<>(context, Optional.empty(), Flux.<Event<T>>empty(), error, node.getPayloadType(), node.getEventType());
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
     * 检查执行是否成功（即没有错误）。
     * @return 如果没有错误则返回 true
     */
    public boolean isSuccess() {
        return error == null;
    }

    /**
     * 检查执行是否失败。
     * @return 如果有错误则返回 true
     */
    public boolean isFailure() {
        return error != null;
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
                Objects.equals(eventType, that.eventType); // 比较 Event 类型
    }

    @Override
    public int hashCode() {
        // 注意：不包含 events (Flux)
        return Objects.hash(context, payload, error, payloadType, eventType); // 包含 Event 类型
    }

    @Override
    public String toString() {
        return "NodeResult{" +
                "context=" + context +
                ", payload=" + payload.map(Object::toString).orElse("<empty>") +
                ", payloadType=" + payloadType.getSimpleName() +
                ", eventType=" + eventType.getSimpleName() + // 显示 Event 类型
                ", hasEvents=" + (events != Flux.<Event<T>>empty()) +
                ", error=" + error +
                '}';
    }
}
