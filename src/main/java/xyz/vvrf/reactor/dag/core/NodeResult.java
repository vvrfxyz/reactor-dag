package xyz.vvrf.reactor.dag.core;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
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

    @Getter
    private final String nodeName;

    /**
     * 私有构造函数，请使用静态工厂方法创建实例。
     */
    private NodeResult(String nodeName, C context, Optional<P> payload, Flux<Event<T>> events, Throwable error, Class<P> payloadType, Class<T> eventType, NodeStatus status) {
        this.nodeName = Objects.requireNonNull(nodeName, "Node name cannot be null");
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
        return new NodeResult<>(node.getName(), context, Optional.ofNullable(payload), Flux.<Event<T>>empty(), null, node.getPayloadType(), node.getEventType(), NodeStatus.SUCCESS);
    }

    /**
     * 创建一个成功的节点结果。
     * 类型通过 DagNode 推断。
     *
     * @param context 上下文对象
     * @param node    产生此结果的 DagNode 实例
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, DagNode<C, P, T> node) {
        Objects.requireNonNull(node, "DagNode 实例不能为空");
        return new NodeResult<>(node.getName(), context, Optional.empty(), Flux.<Event<T>>empty(), null, node.getPayloadType(), node.getEventType(), NodeStatus.SUCCESS);
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
        return new NodeResult<>(node.getName(), context, Optional.empty(), events, null, node.getPayloadType(), node.getEventType(), NodeStatus.SUCCESS);
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
        return new NodeResult<>(node.getName(), context, Optional.ofNullable(payload), events, null, node.getPayloadType(), node.getEventType(),NodeStatus.SUCCESS);
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
        return new NodeResult<>(node.getName(), context, Optional.empty(), Flux.<Event<T>>empty(), error, node.getPayloadType(), node.getEventType(), NodeStatus.FAILURE);
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
        return new NodeResult<>(node.getName(), context, Optional.empty(), Flux.<Event<T>>empty(), null, node.getPayloadType(), node.getEventType(), NodeStatus.SKIPPED); // 设置 SKIPPED
    }

    /**
     * 创建一个失败的节点结果，当无法获取 DagNode 实例时使用（例如配置错误）。
     *
     * @param nodeName    节点名称
     * @param context     上下文对象
     * @param error       执行过程中发生的错误 (不能为空)
     * @param payloadType 预期的 Payload 类型
     * @param eventType   预期的 Event 类型
     * @return NodeResult 实例
     */
    public static <C, P, T> NodeResult<C, P, T> failureForNode(C context, Throwable error, Class<P> payloadType, Class<T> eventType, String nodeName) {
        Objects.requireNonNull(error, "错误对象不能为空");
        Objects.requireNonNull(nodeName, "Node name cannot be null");
        Objects.requireNonNull(payloadType, "Payload 类型不能为空");
        Objects.requireNonNull(eventType, "Event 类型不能为空");
        // 注意：这里 payload 和 events 都是空的
        return new NodeResult<>(nodeName, context, Optional.empty(), Flux.empty(), error, payloadType, eventType, NodeStatus.FAILURE);
    }

    public static <C> NodeResult<C, ?, ?> createFailureResult(C context, Throwable error, DagNode<C, ?, ?> node) {
        Objects.requireNonNull(error, "Error cannot be null for failure result");
        Objects.requireNonNull(node, "DagNode cannot be null for failure result");
        // 调用静态的 callNodeResultFactory
        return callNodeResultFactory(context, node, (ctx, typedNode) -> NodeResult.failure(ctx, error, typedNode));
    }

    /**
     * 辅助方法创建 Skipped NodeResult (使用已知节点实例) - 静态版本
     */
    public static <C> NodeResult<C, ?, ?> createSkippedResult(C context, DagNode<C, ?, ?> node) {
        Objects.requireNonNull(node, "DagNode cannot be null for skipped result");
        // 调用静态的 callNodeResultFactory
        return callNodeResultFactory(context, node, NodeResult::skipped);
    }

    /**
     * NodeResult 工厂方法的函数式接口定义 - 静态版本
     */
    @FunctionalInterface
    private static interface NodeResultFactory<C, N extends DagNode<C, P, T>, P, T> {
        NodeResult<C, P, T> create(C context, N node);
    }

    /**
     * 通用辅助方法，用于调用需要特定类型 DagNode<C, P, T> 的 NodeResult 静态工厂方法 - 静态版本
     */
    @SuppressWarnings("unchecked")
    private static <C, P, T> NodeResult<C, ?, ?> callNodeResultFactory(
            C context,
            DagNode<C, ?, ?> node,
            NodeResultFactory<C, DagNode<C, P, T>, P, T> factory
    ) {
        Objects.requireNonNull(node, "DagNode cannot be null in callNodeResultFactory");
        Objects.requireNonNull(factory, "NodeResultFactory cannot be null");

        try {
            DagNode<C, P, T> typedNode = (DagNode<C, P, T>) node;
            return factory.create(context, typedNode);
        } catch (ClassCastException e) {
            // log 是静态的，可以直接在静态方法中使用
            log.error("Internal error: Failed to cast DagNode '{}' to expected generic type for NodeResult factory. Error: {}",
                    node.getName(), e.getMessage(), e);

            IllegalStateException internalError = new IllegalStateException(
                    "Internal type casting error during NodeResult creation for node '" + node.getName() + "'", e);

            try {
                @SuppressWarnings("unchecked")
                Class<P> payloadType = (Class<P>) node.getPayloadType();
                @SuppressWarnings("unchecked")
                Class<T> eventType = (Class<T>) node.getEventType();
                // 调用 NodeResult 的静态工厂方法
                return NodeResult.failureForNode(context, internalError, payloadType, eventType, node.getName());

            } catch (ClassCastException typeCastEx) {
                log.error("Critical internal error: Failed to cast Class objects obtained from DagNode '{}' for fallback failure result. Error: {}",
                        node.getName(), typeCastEx.getMessage(), typeCastEx);
                IllegalStateException criticalError = new IllegalStateException(
                        "Critical internal type casting error (Class objects) for node '" + node.getName() + "'", typeCastEx);
                // 调用 NodeResult 的静态工厂方法
                return NodeResult.failureForNode(context, criticalError, (Class<P>)Object.class, (Class<T>)Object.class, node.getName());
            }
        }
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
        // 不比较 events (Flux) 和 context
        return nodeName.equals(that.nodeName) &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(error, that.error) &&
                payloadType.equals(that.payloadType) &&
                eventType.equals(that.eventType) &&
                status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeName, payload, error, payloadType, eventType, status);
    }

    @Override
    public String toString() {
        return "NodeResult{" +
                "nodeName='" + nodeName + '\'' +
                ", status=" + status +
                ", payload=" + payload.map(Object::toString).orElse("<empty>") +
                ", payloadType=" + payloadType.getSimpleName() +
                ", eventType=" + eventType.getSimpleName() +
                ", hasEvents=" + (events != Flux.<Event<T>>empty() && status == NodeStatus.SUCCESS) +
                ", error=" + error +
                '}';
    }
}
