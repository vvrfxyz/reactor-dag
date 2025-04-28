package xyz.vvrf.reactor.dag.core;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.util.Objects;
import java.util.Optional;

/**
 * 节点执行结果 - 包含上下文、可选的 Payload、事件流和可能的错误。
 * 使用 Builder 模式创建实例。
 *
 * @param <C> 上下文类型 (Context Type)
 * @param <P> 结果 Payload 类型 (Payload Type)
 * <T> 结果事件数据类型 (Event Data Type)
 * @author ruifeng.wen (Optimized by AI Assistant)
 */
@Slf4j
@Getter
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

    private final String nodeName;
    private final C context;
    private final Optional<P> payload;
    private final Flux<Event<T>> events;
    private final Throwable error;
    private final Class<P> payloadType;
    private final Class<T> eventType;
    private final NodeStatus status;

    /**
     * 私有构造函数，请使用 Builder 创建实例。
     */
    private NodeResult(Builder<C, P, T> builder) {
        this.nodeName = Objects.requireNonNull(builder.nodeName, "Node name cannot be null");
        this.context = builder.context; // Context 可以为 null 吗？根据原代码，它不是必须的，但这里保持非 null
        this.payload = Objects.requireNonNull(builder.payload, "Payload Optional cannot be null");
        this.events = Objects.requireNonNull(builder.events, "Events Flux cannot be null (use Flux.empty() instead)");
        this.error = builder.error; // Error 可以为 null
        this.payloadType = Objects.requireNonNull(builder.payloadType, "Payload type cannot be null");
        this.eventType = Objects.requireNonNull(builder.eventType, "Event type cannot be null");
        this.status = Objects.requireNonNull(builder.status, "NodeStatus cannot be null");

        // 校验：失败状态必须有 error，成功/跳过状态不能有 error
        if (this.status == NodeStatus.FAILURE && this.error == null) {
            throw new IllegalArgumentException("FAILURE status requires a non-null error for node: " + this.nodeName);
        }
        if ((this.status == NodeStatus.SUCCESS || this.status == NodeStatus.SKIPPED) && this.error != null) {
            log.warn("NodeResult for node '{}' has status {} but also contains an error. Error will be ignored for status checks.",
                    this.nodeName, this.status, this.error);
            // 或者可以选择抛出异常，取决于业务逻辑严格性
            // throw new IllegalArgumentException("SUCCESS/SKIPPED status cannot have an error for node: " + this.nodeName);
        }
        // 校验：非成功状态不应有实际的 Payload 或 Events (允许 Optional.empty() 和 Flux.empty())
        if (this.status != NodeStatus.SUCCESS) {
            if (this.payload.isPresent()) {
                log.warn("NodeResult for node '{}' has status {} but contains a payload. Payload might be unexpected.",
                        this.nodeName, this.status);
            }
        }
    }

    // --- Builder ---

    /**
     * NodeResult 的构建器。
     *
     * @param <C> 上下文类型
     * @param <P> Payload 类型
     * <T> 事件数据类型
     */
    public static class Builder<C, P, T> {
        private String nodeName;
        private C context;
        private Optional<P> payload = Optional.empty(); // 默认为空
        private Flux<Event<T>> events = Flux.empty();   // 默认为空 Flux
        private Throwable error;
        private Class<P> payloadType;
        private Class<T> eventType;
        private NodeStatus status;

        // 私有构造函数，通过静态方法 newBuilder 获取
        private Builder(String nodeName, C context, Class<P> payloadType, Class<T> eventType) {
            this.nodeName = nodeName;
            this.context = context; // 允许 context 为 null 吗？如果允许，需要调整 NodeResult 的构造函数和字段
            this.payloadType = payloadType;
            this.eventType = eventType;
        }

        public Builder<C, P, T> payload(P payload) {
            this.payload = Optional.ofNullable(payload);
            return this;
        }

        public Builder<C, P, T> events(Flux<Event<T>> events) {
            this.events = (events != null) ? events : Flux.empty();
            return this;
        }

        // 内部使用，设置失败状态和错误
        private Builder<C, P, T> failure(Throwable error) {
            this.status = NodeStatus.FAILURE;
            this.error = Objects.requireNonNull(error, "Error cannot be null for failure status");
            this.payload = Optional.empty(); // 失败时不应有 payload
            this.events = Flux.empty();      // 失败时不应有 events
            return this;
        }

        // 内部使用，设置成功状态
        private Builder<C, P, T> success() {
            this.status = NodeStatus.SUCCESS;
            this.error = null; // 成功状态清除错误
            return this;
        }

        // 内部使用，设置跳过状态
        private Builder<C, P, T> skipped() {
            this.status = NodeStatus.SKIPPED;
            this.error = null;
            this.payload = Optional.empty();
            this.events = Flux.empty();
            return this;
        }

        // 设置自定义状态 (谨慎使用)
        public Builder<C, P, T> status(NodeStatus status) {
            this.status = status;
            // 根据状态重置 error/payload/events (可选，但推荐)
            if (status == NodeStatus.FAILURE) {
                if (this.error == null) throw new IllegalStateException("Cannot set status to FAILURE without providing an error first via factory method.");
                this.payload = Optional.empty();
                this.events = Flux.empty();
            } else {
                this.error = null;
                if (status == NodeStatus.SKIPPED) {
                    this.payload = Optional.empty();
                    this.events = Flux.empty();
                }
            }
            return this;
        }


        public NodeResult<C, P, T> build() {
            // 可以在这里添加更多校验逻辑，确保 Builder 状态一致性
            if (status == null) {
                throw new IllegalStateException("NodeResult status must be set before building (e.g., via success(), failure(), skipped() factory methods or status())");
            }
            return new NodeResult<>(this);
        }
    }

    // --- 静态工厂方法 (使用 Builder) ---

    /**
     * 创建一个 NodeResult 构建器，需要提供基础信息。
     *
     * @param nodeName    节点名称
     * @param context     上下文
     * @param payloadType Payload 类型
     * @param eventType   Event 类型
     * @return Builder 实例
     */
    public static <C, P, T> Builder<C, P, T> builder(String nodeName, C context, Class<P> payloadType, Class<T> eventType) {
        Objects.requireNonNull(nodeName, "Node name cannot be null for builder");
        Objects.requireNonNull(payloadType, "Payload type cannot be null for builder");
        Objects.requireNonNull(eventType, "Event type cannot be null for builder");
        // Context 是否允许为 null 取决于业务需求，这里假设允许
        // Objects.requireNonNull(context, "Context cannot be null for builder");
        return new Builder<>(nodeName, context, payloadType, eventType);
    }

    /**
     * 创建一个 NodeResult 构建器，从 DagNode 推断类型和名称。
     * 这是类型安全的，避免了原始代码中的强制转换问题。
     *
     * @param context 上下文
     * @param node    DagNode 实例 (可以是 DagNode<C, ?, ?>)
     * @return Builder 实例
     */
    @SuppressWarnings("unchecked") // Casts from Class<?> are necessary due to generics
    public static <C, P, T> Builder<C, P, T> builder(C context, DagNode<C, ?, ?> node) {
        Objects.requireNonNull(node, "DagNode cannot be null for builder");
        Class<P> pType;
        Class<T> eType;
        try {
            // 从 Node 获取类型信息。这里的转换是必要的，因为 node 是 ? 类型。
            // 如果 node.getPayloadType() 返回的不是 Class<P>，会在运行时失败，
            // 但这比在 callNodeResultFactory 中隐藏转换要清晰。
            pType = (Class<P>) node.getPayloadType();
            eType = (Class<T>) node.getEventType();
        } catch (ClassCastException e) {
            log.error("Failed to cast types from DagNode '{}'. Expected P={}, T={}. Got P={}, T={}. Error: {}",
                    node.getName(), "P", "T", node.getPayloadType(), node.getEventType(), e.getMessage(), e);
            // 抛出更明确的异常，而不是尝试创建错误的 FailureResult
            throw new IllegalArgumentException("Type mismatch when creating NodeResult builder from DagNode '" + node.getName() + "'", e);
        }
        return new Builder<>(node.getName(), context, pType, eType);
    }

    /**
     * 创建一个成功的节点结果。
     *
     * @param context 上下文
     * @param node    产生此结果的 DagNode 实例
     * @return NodeResult 实例 (状态: SUCCESS)
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, DagNode<C, P, T> node) {
        return NodeResult.<C, P, T>builder(context, node)
                .success()
                .build();
    }

    /**
     * 创建一个成功的节点结果，包含 Payload。
     *
     * @param context 上下文
     * @param payload 结果 Payload (可以为 null)
     * @param node    产生此结果的 DagNode 实例
     * @return NodeResult 实例 (状态: SUCCESS)
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, P payload, DagNode<C, P, T> node) {
        return NodeResult.<C, P, T>builder(context, node)
                .payload(payload)
                .success()
                .build();
    }

    /**
     * 创建一个成功的节点结果，包含事件流。
     *
     * @param context 上下文
     * @param events  事件流 (不能为空，若无事件应传入 Flux.empty())
     * @param node    产生此结果的 DagNode 实例
     * @return NodeResult 实例 (状态: SUCCESS)
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, Flux<Event<T>> events, DagNode<C, P, T> node) {
        Objects.requireNonNull(events, "Events Flux cannot be null for success result");
        return NodeResult.<C, P, T>builder(context, node)
                .events(events)
                .success()
                .build();
    }

    /**
     * 创建一个成功的节点结果，包含 Payload 和事件流。
     *
     * @param context 上下文
     * @param payload 结果 Payload (可以为 null)
     * @param events  事件流 (不能为空，若无事件应传入 Flux.empty())
     * @param node    产生此结果的 DagNode 实例
     * @return NodeResult 实例 (状态: SUCCESS)
     */
    public static <C, P, T> NodeResult<C, P, T> success(C context, P payload, Flux<Event<T>> events, DagNode<C, P, T> node) {
        Objects.requireNonNull(events, "Events Flux cannot be null for success result");
        return NodeResult.<C, P, T>builder(context, node)
                .payload(payload)
                .events(events)
                .success()
                .build();
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
        Objects.requireNonNull(error, "Error cannot be null for failure result");
        return NodeResult.<C, P, T>builder(nodeName, context, payloadType, eventType)
                .failure(error)
                .build();
    }

    /**
     * 创建一个失败的节点结果，当可以提供类型明确的 DagNode 时使用。
     * 返回类型具有正确的 P 和 T 泛型。
     *
     * @param <C> 上下文类型
     * @param <P> Payload 类型
     * @param <T> Event 数据类型
     * @param context 上下文
     * @param error   发生的错误 (不能为空)
     * @param node    尝试执行并失败的 DagNode<C, P, T> 实例 (类型必须匹配, 不能为空)
     * @return NodeResult<C, P, T> 实例 (状态: FAILURE)
     */
    public static <C, P, T> NodeResult<C, P, T> failure(C context, Throwable error, DagNode<C, P, T> node) {
        Objects.requireNonNull(error, "Error cannot be null for failure result");
        Objects.requireNonNull(node, "Node cannot be null for failure result");
        // 因为 node 的类型是 DagNode<C, P, T>，builder 可以正确推断类型
        return NodeResult.<C, P, T>builder(context, node)
                .failure(error)
                .build();
    }


    /**
     * 创建一个表示节点被跳过的结果。
     *
     * @param context 上下文
     * @param node    被跳过的 DagNode 实例 (可以是 DagNode<C, ?, ?>)
     * @return NodeResult 实例 (状态: SKIPPED)
     */
    public static <C> NodeResult<C, ?, ?> skipped(C context, DagNode<C, ?, ?> node) {
        // 使用 builder(context, node) 来安全地获取类型和名称
        return NodeResult.builder(context, node) // builder 会推断 P 和 T
                .skipped() // skipped() 内部会设置状态并清空 payload/events/error
                .build();
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

    // --- equals, hashCode, toString --- (保持不变或稍作调整)

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeResult<?, ?, ?> that = (NodeResult<?, ?, ?>) o;
        // 不比较 events (Flux) 和 context
        return nodeName.equals(that.nodeName) &&
                Objects.equals(payload, that.payload) && // Optional 的 equals 比较的是内容
                Objects.equals(error, that.error) &&
                payloadType.equals(that.payloadType) &&
                eventType.equals(that.eventType) &&
                status == that.status;
    }

    @Override
    public int hashCode() {
        // 不包含 events 和 context
        return Objects.hash(nodeName, payload, error, payloadType, eventType, status);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("NodeResult{");
        sb.append("nodeName='").append(nodeName).append('\'');
        sb.append(", status=").append(status);
        payload.ifPresent(p -> sb.append(", payload=").append(p)); // 更简洁地显示 payload
        sb.append(", payloadType=").append(payloadType.getSimpleName());
        sb.append(", eventType=").append(eventType.getSimpleName());
        // 更准确地判断是否有事件，但避免订阅 Flux
        boolean hasEvents = events != (Flux<?>)Flux.empty(); // 基础检查
        sb.append(", hasEvents=").append(hasEvents);
        if (error != null) {
            sb.append(", error=").append(error);
        }
        // Context 通常比较复杂，可以选择不打印或只打印类名
        // if (context != null) {
        //     sb.append(", contextType=").append(context.getClass().getSimpleName());
        // }
        sb.append('}');
        return sb.toString();
    }
}
