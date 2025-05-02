// [file name]: NodeResult.java
package xyz.vvrf.reactor.dag.core;

import lombok.Getter;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.Optional;

/**
 * 节点执行结果 - 包含上下文、事件流和可能的错误。
 * 不再包含 Payload。
 *
 * @param <C> 上下文类型 (Context Type)
 * @param <T> 结果事件数据类型 (Event Data Type)
 */
public final class NodeResult<C, T> {

    public enum NodeStatus {
        SUCCESS, FAILURE, SKIPPED
    }

    @Getter
    private final C context; // 保留 Context 引用，可能用于日志或特定场景

    @Getter
    private final Flux<Event<T>> events; // 事件流

    private final Throwable error; // 失败时的错误

    @Getter
    private final Class<T> eventType; // 事件类型

    @Getter
    private final NodeStatus status; // 节点状态

    /**
     * 私有构造函数，请使用静态工厂方法创建实例。
     */
    private NodeResult(C context, Flux<Event<T>> events, Throwable error, Class<T> eventType, NodeStatus status) {
        this.context = context; // Context 可以为 null 吗？取决于业务，暂时允许
        this.events = Objects.requireNonNull(events, "事件流 Flux 不能为 null (可以是 Flux.empty())");
        this.error = error; // 允许为 null
        this.eventType = Objects.requireNonNull(eventType, "Event 类型不能为空");
        this.status = Objects.requireNonNull(status, "NodeStatus 不能为空");

        if (status == NodeStatus.FAILURE && error == null) {
            throw new IllegalArgumentException("FAILURE 状态必须有关联的 Throwable error");
        }
        if (status != NodeStatus.FAILURE && error != null) {
            throw new IllegalArgumentException("只有 FAILURE 状态才能有关联的 Throwable error");
        }
        if (status == NodeStatus.SKIPPED && events != Flux.<Event<T>>empty()) {
            // 理论上 skipped 节点不应产生事件，但如果逻辑复杂允许了，这里可以只打警告
            // System.err.println("Warning: SKIPPED NodeResult created with non-empty events Flux.");
        }
    }

    /**
     * 创建一个成功的节点结果，包含事件流。
     *
     * @param context   上下文对象
     * @param events    节点产生的事件流 (不能为空, 可以是 Flux.empty())
     * @param eventType 结果的 Event 类型 Class 对象
     * @return NodeResult 实例
     */
    public static <C, T> NodeResult<C, T> success(C context, Flux<Event<T>> events, Class<T> eventType) {
        return new NodeResult<>(context, events, null, eventType, NodeStatus.SUCCESS);
    }

    /**
     * 创建一个失败的节点结果。
     *
     * @param context   上下文对象
     * @param error     执行过程中发生的错误 (不能为空)
     * @param eventType 结果的 Event 类型 Class 对象 (即使失败也要提供，用于类型系统)
     * @return NodeResult 实例
     */
    public static <C, T> NodeResult<C, T> failure(C context, Throwable error, Class<T> eventType) {
        return new NodeResult<>(context, Flux.empty(), error, eventType, NodeStatus.FAILURE);
    }

    /**
     * 创建一个表示节点被跳过的结果。
     *
     * @param context   上下文对象
     * @param eventType 结果的 Event 类型 Class 对象 (即使跳过也要提供)
     * @return NodeResult 实例，状态为 SKIPPED
     */
    public static <C, T> NodeResult<C, T> skipped(C context, Class<T> eventType) {
        return new NodeResult<>(context, Flux.empty(), null, eventType, NodeStatus.SKIPPED);
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeResult<?, ?> that = (NodeResult<?, ?>) o;
        // 注意：不比较 events (Flux) 和 context
        return Objects.equals(error, that.error) &&
                eventType.equals(that.eventType) &&
                status == that.status;
    }

    @Override
    public int hashCode() {
        // 注意：不包含 events 和 context
        return Objects.hash(error, eventType, status);
    }

    @Override
    public String toString() {
        return "NodeResult{" +
                "status=" + status +
                ", eventType=" + eventType.getSimpleName() +
                ", hasEvents=" + (events != (Flux<?>)Flux.empty() && status == NodeStatus.SUCCESS) +
                ", error=" + error +
                // ", context=" + context + // Context 可能很大，通常不打印
                '}';
    }
}
