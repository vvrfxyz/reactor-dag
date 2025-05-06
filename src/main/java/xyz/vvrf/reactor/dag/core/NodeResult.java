package xyz.vvrf.reactor.dag.core;

import lombok.Getter;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.Optional;

/**
 * 节点执行结果 - 包含可选的 Payload、事件流和可能的错误。
 * 不再包含 Context 和 payloadType。
 *
 * @param <C> 上下文类型 (仅用于静态工厂方法的类型推断，结果本身不持有)
 * @param <P> 结果 Payload 类型 (Payload Type)
 * @author Refactored
 */
public final class NodeResult<C, P> { // C 仅用于工厂方法签名

    public enum NodeStatus {
        SUCCESS, FAILURE, SKIPPED
    }

    // 不再持有 context
    // @Getter private final C context;

    @Getter
    private final Optional<P> payload; // Payload 对应节点的默认 OutputSlot

    @Getter
    private final Flux<Event<?>> events;

    private final Throwable error;

    // 不再持有 payloadType
    // @Getter private final Class<P> payloadType;

    @Getter
    private final NodeStatus status;

    /**
     * 私有构造函数，请使用静态工厂方法创建实例。
     */
    private NodeResult(Optional<P> payload, Flux<Event<?>> events, Throwable error, NodeStatus status) {
        // this.context = context; // Removed
        this.payload = Objects.requireNonNull(payload, "Payload Optional 不能为 null");
        this.events = (events != null) ? events : Flux.empty();
        this.error = error;
        // this.payloadType = Objects.requireNonNull(payloadType, "Payload 类型不能为空"); // Removed
        this.status = Objects.requireNonNull(status, "NodeStatus 不能为空");

        // 验证逻辑保持不变
        if (status == NodeStatus.FAILURE && error == null) {
            throw new IllegalArgumentException("FAILURE status requires a non-null error.");
        }
        if (status != NodeStatus.FAILURE && error != null) {
            throw new IllegalArgumentException("Non-FAILURE status cannot have an error.");
        }
        if (status == NodeStatus.SKIPPED && payload.isPresent()) {
            System.err.printf("Warning: SKIPPED NodeResult created with a present payload.%n");
        }
    }

    // --- 静态工厂方法 (移除了 context 和 payloadType 参数) ---

    public static <C, P> NodeResult<C, P> success(P payload) {
        return new NodeResult<>(Optional.ofNullable(payload), Flux.empty(), null, NodeStatus.SUCCESS);
    }

    public static <C, P> NodeResult<C, P> success() {
        return new NodeResult<>(Optional.empty(), Flux.empty(), null, NodeStatus.SUCCESS);
    }

    public static <C, P> NodeResult<C, P> success(Flux<Event<?>> events) {
        Objects.requireNonNull(events, "事件流不能为空");
        return new NodeResult<>(Optional.empty(), events, null, NodeStatus.SUCCESS);
    }

    public static <C, P> NodeResult<C, P> success(P payload, Flux<Event<?>> events) {
        Objects.requireNonNull(events, "事件流不能为空");
        return new NodeResult<>(Optional.ofNullable(payload), events, null, NodeStatus.SUCCESS);
    }

    public static <C, P> NodeResult<C, P> failure(Throwable error) {
        Objects.requireNonNull(error, "错误对象不能为空");
        return new NodeResult<>(Optional.empty(), Flux.empty(), error, NodeStatus.FAILURE);
    }

    public static <C, P> NodeResult<C, P> skipped() {
        return new NodeResult<>(Optional.empty(), Flux.empty(), null, NodeStatus.SKIPPED);
    }

    // --- 实例方法 (保持不变) ---

    public Optional<Throwable> getError() {
        return Optional.ofNullable(error);
    }

    public boolean isSuccess() { return this.status == NodeStatus.SUCCESS; }
    public boolean isFailure() { return this.status == NodeStatus.FAILURE; }
    public boolean isSkipped() { return this.status == NodeStatus.SKIPPED; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeResult<?, ?> that = (NodeResult<?, ?>) o;
        // 不比较 events
        return Objects.equals(payload, that.payload) &&
                Objects.equals(error, that.error) &&
                status == that.status;
    }

    @Override
    public int hashCode() {
        // 不包含 events
        return Objects.hash(payload, error, status);
    }

    @Override
    public String toString() {
        return "NodeResult{" +
                "status=" + status +
                ", payload=" + payload.map(p -> p.getClass().getSimpleName()).orElse("<empty>") +
                ", hasEvents=" + (events != Flux.<Event<?>>empty()) +
                ", error=" + error +
                '}';
    }
}