package xyz.vvrf.reactor.dag.core;

import lombok.Getter;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.Optional;

/**
 * 代表单个 DAG 节点执行完成后的结果（不可变数据类）。
 * 包含执行状态、可选的主输出 Payload、可选的事件流以及可能的错误信息。
 *
 * @param <C> 上下文类型 (仅用于静态工厂方法的类型推断，结果本身不持有 Context)
 * @param <P> 结果 Payload 类型 (对应节点的主输出槽)
 * @author Refactored (注释更新)
 */
public final class NodeResult<C, P> { // C 仅用于工厂方法签名

    /**
     * 节点执行状态枚举。
     */
    public enum NodeStatus {
        /** 节点成功执行。可能包含 Payload 和/或事件。*/
        SUCCESS,
        /** 节点执行失败。必须包含错误信息。不应包含 Payload。*/
        FAILURE,
        /** 节点被跳过。通常因为上游失败/跳过或边条件不满足。不应包含 Payload 或错误。*/
        SKIPPED
    }

    @Getter private final NodeStatus status;
    @Getter private final Optional<P> payload; // 主输出 Payload
    @Getter private final Flux<Event<?>> events; // 产生的事件流
    private final Throwable error; // 仅在 status 为 FAILURE 时存在

    /**
     * 私有构造函数，请使用静态工厂方法创建实例。
     */
    private NodeResult(NodeStatus status, Optional<P> payload, Flux<Event<?>> events, Throwable error) {
        this.status = Objects.requireNonNull(status, "节点状态不能为空");
        this.payload = Objects.requireNonNull(payload, "Payload Optional 不能为 null");
        this.events = (events != null) ? events : Flux.empty(); // 确保 events 不为 null
        this.error = error;

        // 内部一致性校验
        if (status == NodeStatus.FAILURE && error == null) {
            throw new IllegalArgumentException("FAILURE 状态的结果必须包含一个非空的错误信息。");
        }
        if (status != NodeStatus.FAILURE && error != null) {
            throw new IllegalArgumentException("非 FAILURE 状态的结果不能包含错误信息。");
        }
        if (status == NodeStatus.SKIPPED && payload.isPresent()) {
            // 允许但不推荐 SKIPPED 带有 payload，打印警告
            System.err.printf("警告: 创建了一个 SKIPPED 状态的 NodeResult，但其包含了 Payload: %s%n", payload.get());
        }
        if (status == NodeStatus.SKIPPED && events != Flux.<Event<?>>empty()) {
            // 允许但不推荐 SKIPPED 带有 events，打印警告
            System.err.println("警告: 创建了一个 SKIPPED 状态的 NodeResult，但其包含了事件流。");
        }
    }

    // --- 静态工厂方法 ---

    /**
     * 创建一个表示成功执行的结果，包含 Payload。
     * @param payload 节点的主输出 Payload (可以为 null)
     */
    public static <C, P> NodeResult<C, P> success(P payload) {
        return new NodeResult<>(NodeStatus.SUCCESS, Optional.ofNullable(payload), Flux.empty(), null);
    }

    /**
     * 创建一个表示成功执行的结果，不包含 Payload。
     */
    public static <C, P> NodeResult<C, P> success() {
        return new NodeResult<>(NodeStatus.SUCCESS, Optional.empty(), Flux.empty(), null);
    }

    /**
     * 创建一个表示成功执行的结果，只包含事件流。
     * @param events 节点产生的事件流 (不能为空)
     */
    public static <C, P> NodeResult<C, P> success(Flux<Event<?>> events) {
        Objects.requireNonNull(events, "事件流不能为空");
        return new NodeResult<>(NodeStatus.SUCCESS, Optional.empty(), events, null);
    }

    /**
     * 创建一个表示成功执行的结果，包含 Payload 和事件流。
     * @param payload 节点的主输出 Payload (可以为 null)
     * @param events 节点产生的事件流 (不能为空)
     */
    public static <C, P> NodeResult<C, P> success(P payload, Flux<Event<?>> events) {
        Objects.requireNonNull(events, "事件流不能为空");
        return new NodeResult<>(NodeStatus.SUCCESS, Optional.ofNullable(payload), events, null);
    }

    /**
     * 创建一个表示执行失败的结果。
     * @param error 导致失败的异常 (不能为空)
     */
    public static <C, P> NodeResult<C, P> failure(Throwable error) {
        Objects.requireNonNull(error, "错误对象不能为空");
        return new NodeResult<>(NodeStatus.FAILURE, Optional.empty(), Flux.empty(), error);
    }

    /**
     * 创建一个表示节点被跳过的结果。
     */
    public static <C, P> NodeResult<C, P> skipped() {
        return new NodeResult<>(NodeStatus.SKIPPED, Optional.empty(), Flux.empty(), null);
    }

    // --- 实例方法 ---

    /**
     * 获取错误信息（仅当状态为 FAILURE 时存在）。
     * @return 包含错误的 Optional。
     */
    public Optional<Throwable> getError() {
        return Optional.ofNullable(error);
    }

    public boolean isSuccess() { return this.status == NodeStatus.SUCCESS; }
    public boolean isFailure() { return this.status == NodeStatus.FAILURE; }
    public boolean isSkipped() { return this.status == NodeStatus.SKIPPED; }

    // --- equals, hashCode, toString ---

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeResult<?, ?> that = (NodeResult<?, ?>) o;
        // 注意：不比较 events (Flux 的比较通常没有意义)
        return status == that.status &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        // 注意：不包含 events
        return Objects.hash(status, payload, error);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("NodeResult{");
        sb.append("status=").append(status);
        payload.ifPresent(p -> sb.append(", payload=").append(p.getClass().getSimpleName()));
        // 不直接打印 events 内容，只标记是否存在
        if (events != Flux.<Event<?>>empty()) {
            sb.append(", hasEvents=true");
        }
        getError().ifPresent(e -> sb.append(", error=").append(e.getClass().getSimpleName()));
        sb.append('}');
        return sb.toString();
    }
}
