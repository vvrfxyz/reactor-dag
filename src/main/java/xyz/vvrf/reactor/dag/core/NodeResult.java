package xyz.vvrf.reactor.dag.core;

import lombok.Getter;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.Optional;

/**
 * 节点执行结果 - 包含上下文、事件流和可能的错误
 *
 * @param <C> 上下文类型 (Context Type)
 * @param <T> 结果负载类型 (Payload Type)
 * @author ruifeng.wen
 */
public final class NodeResult<C, T> {

    @Getter
    private final C context;

    @Getter
    private final Flux<Event<T>> events;

    private final Throwable error;

    @Getter
    private final Class<T> resultType;

    /**
     * 创建无错误的节点结果
     *
     * @param context 上下文对象
     * @param events 事件流
     * @param resultType 结果类型
     */
    public NodeResult(C context, Flux<Event<T>> events, Class<T> resultType) {
        this(context, events, null, resultType);
    }

    /**
     * 创建可能包含错误的节点结果
     *
     * @param context 上下文对象
     * @param events 事件流
     * @param error 错误对象，如果没有错误则为null
     * @param resultType 结果类型
     */
    public NodeResult(C context, Flux<Event<T>> events, Throwable error, Class<T> resultType) {
        this.context = context;
        this.events = (events != null) ? events : Flux.empty();
        this.error = error;
        this.resultType = Objects.requireNonNull(resultType, "结果类型不能为空");
    }

    /**
     * 获取执行过程中的错误
     *
     * @return 包含错误的Optional，如果没有错误则为空
     */
    public Optional<Throwable> getError() {
        return Optional.ofNullable(error);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeResult<?, ?> that = (NodeResult<?, ?>) o;
        return Objects.equals(context, that.context) &&
                Objects.equals(error, that.error) &&
                Objects.equals(resultType, that.resultType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, error, resultType);
    }

    @Override
    public String toString() {
        return "NodeResult{" +
                "context=" + context +
                ", error=" + error +
                ", resultType=" + resultType.getSimpleName() +
                '}';
    }
}