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
    private final Optional<P> payload; // 节点的主要数据产物

    @Getter
    private final Flux<Event<T>> events; // 节点产生的事件流

    private final Throwable error; // 执行过程中发生的错误

    @Getter
    private final Class<P> payloadType; // 预期的 Payload 类型

    /**
     * 私有构造函数，请使用静态工厂方法创建实例。
     */
    private NodeResult(C context, Optional<P> payload, Flux<Event<T>> events, Throwable error, Class<P> payloadType) {
        this.context = context;
        this.payload = Objects.requireNonNull(payload, "Payload Optional 不能为 null");
        this.events = (events != null) ? events : Flux.empty();
        this.error = error;
        this.payloadType = Objects.requireNonNull(payloadType, "Payload 类型不能为空");
    }

    /**
     * 创建一个成功的节点结果，只包含 Payload。
     *
     * @param context     上下文对象
     * @param payload     节点产出的 Payload 数据 (可以为 null)
     * @param payloadType 预期的 Payload 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, P payload, Class<P> payloadType, Class<T> eventType) {
        Objects.requireNonNull(payloadType, "Payload 类型不能为空");
        Objects.requireNonNull(eventType, "事件类型不能为空");
        return new NodeResult<>(context, Optional.ofNullable(payload), Flux.<Event<T>>empty(), null, payloadType);
    }

    /**
     * 创建一个成功的节点结果，只包含事件流。
     *
     * @param context     上下文对象
     * @param events      节点产生的事件流 (不能为空)
     * @param payloadType 预期的 Payload 类型 Class 对象 (即使没有 Payload，也需指定节点声明的类型)
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, Flux<Event<T>> events, Class<P> payloadType) {
        Objects.requireNonNull(events, "事件流不能为空");
        return new NodeResult<>(context, Optional.empty(), events, null, payloadType);
    }

    /**
     * 创建一个成功的节点结果，同时包含 Payload 和事件流。
     *
     * @param context     上下文对象
     * @param payload     节点产出的 Payload 数据 (可以为 null)
     * @param events      节点产生的事件流 (不能为空)
     * @param payloadType 预期的 Payload 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, P payload, Flux<Event<T>> events, Class<P> payloadType) {
        Objects.requireNonNull(events, "事件流不能为空");
        return new NodeResult<>(context, Optional.ofNullable(payload), events, null, payloadType);
    }

    /**
     * 创建一个失败的节点结果。
     *
     * @param context     上下文对象
     * @param error       执行过程中发生的错误 (不能为空)
     * @param payloadType 预期的 Payload 类型 Class 对象
     * @param eventType   预期的 Event 类型 Class 对象 (新增参数) <--- 新增
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> failure(C context, Throwable error, Class<P> payloadType, Class<T> eventType) {
        Objects.requireNonNull(error, "错误对象不能为空");
        Objects.requireNonNull(payloadType, "Payload 类型不能为空");
        Objects.requireNonNull(eventType, "事件类型不能为空");

        return new NodeResult<>(context, Optional.empty(), Flux.<Event<T>>empty(), error, payloadType);
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
                Objects.equals(payload, that.payload) && // 比较 Optional<P>
                Objects.equals(error, that.error) &&
                Objects.equals(payloadType, that.payloadType); // 比较 Class<P>
    }

    @Override
    public int hashCode() {
        // 注意：不包含 events (Flux)
        return Objects.hash(context, payload, error, payloadType);
    }

    @Override
    public String toString() {
        return "NodeResult{" +
                "context=" + context +
                ", payload=" + payload.map(Object::toString).orElse("<empty>") + // 显示 payload 内容或 <empty>
                ", payloadType=" + payloadType.getSimpleName() +
                ", hasEvents=" + (events != Flux.<Event<T>>empty()) + // 指示是否有事件流
                ", error=" + error +
                '}';
    }
}
