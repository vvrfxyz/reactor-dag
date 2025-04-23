package xyz.vvrf.reactor.dag.test.util; // 放在测试工具包下

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import xyz.vvrf.reactor.dag.core.*;

import java.time.Duration;
import java.util.*;
import java.util.function.BiFunction; // 引入 BiFunction
// import java.util.function.Function; // 不再需要 Function

/**
 * 用于测试目的的可配置 DagNode 实现。
 * 使用 Builder 模式进行配置。
 *
 * @param <C> 上下文类型
 * @param <P> Payload 类型
 * @param <T> Event 类型
 */
public class TestDagNode<C, P, T> implements DagNode<C, P, T> {

    private final String name;
    private final Class<P> payloadType;
    private final Class<T> eventType;
    private final List<DependencyDescriptor> dependencies;
    private final BiFunction<C, DependencyAccessor<C>, Boolean> shouldExecuteLogic;
    private final BiFunction<C, DependencyAccessor<C>, Mono<NodeResult<C, P, T>>> executionLogic;
    private final Retry retrySpec;
    private final Duration executionTimeout;

    private TestDagNode(Builder<C, P, T> builder) {
        this.name = Objects.requireNonNull(builder.name, "节点名称不能为空");
        this.payloadType = Objects.requireNonNull(builder.payloadType, "Payload 类型不能为空");
        this.eventType = Objects.requireNonNull(builder.eventType, "Event 类型不能为空");
        this.dependencies = Collections.unmodifiableList(new ArrayList<>(builder.dependencies));
        this.shouldExecuteLogic = builder.shouldExecuteLogic;
        this.executionLogic = builder.executionLogic;
        this.retrySpec = builder.retrySpec;
        this.executionTimeout = builder.executionTimeout;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<DependencyDescriptor> getDependencies() {
        return dependencies;
    }

    @Override
    public Retry getRetrySpec() {
        return retrySpec;
    }

    @Override
    public boolean shouldExecute(C context, DependencyAccessor<C> dependencies) {
        return shouldExecuteLogic.apply(context, dependencies);
    }

    @Override
    public Class<P> getPayloadType() {
        return payloadType;
    }

    @Override
    public Class<T> getEventType() {
        return eventType;
    }

    @Override
    public Duration getExecutionTimeout() {
        return executionTimeout;
    }

    @Override
    public Mono<NodeResult<C, P, T>> execute(C context, DependencyAccessor<C> dependencies) {
        try {
            // 默认行为：如果未提供执行逻辑，则返回一个成功的、空的 NodeResult
            if (executionLogic == null) {
                // 使用 NodeResult 的工厂方法，需要传入 this (DagNode 实例)
                return Mono.just(NodeResult.success(context, (P) null, this));
            }
            // 执行提供的逻辑
            return executionLogic.apply(context, dependencies);
        } catch (Exception e) {
            // 如果提供的逻辑直接抛出异常，包装成失败的 Mono
            return Mono.just(NodeResult.failure(context, e, this));
        }
    }

    public static <C, P, T> Builder<C, P, T> builder(String name, Class<P> payloadType, Class<T> eventType) {
        return new Builder<>(name, payloadType, eventType);
    }

    public static class Builder<C, P, T> {
        private final String name;
        private final Class<P> payloadType;
        private final Class<T> eventType;
        private List<DependencyDescriptor> dependencies = new ArrayList<>();
        private BiFunction<C, DependencyAccessor<C>, Boolean> shouldExecuteLogic = (ctx, deps) -> true;
        private BiFunction<C, DependencyAccessor<C>, Mono<NodeResult<C, P, T>>> executionLogic = null;
        private Retry retrySpec = null;
        private Duration executionTimeout = null;

        Builder(String name, Class<P> payloadType, Class<T> eventType) {
            this.name = name;
            this.payloadType = payloadType;
            this.eventType = eventType;
        }

        public Builder<C, P, T> dependencies(List<DependencyDescriptor> dependencies) {
            this.dependencies = new ArrayList<>(dependencies);
            return this;
        }

        public Builder<C, P, T> addDependency(String nodeName, Class<?> requiredType) {
            this.dependencies.add(new DependencyDescriptor(nodeName, requiredType));
            return this;
        }

        /**
         * 设置节点的条件执行逻辑。
         * @param shouldExecuteLogic 一个函数，接收上下文和依赖访问器，返回 true 表示应执行，false 表示应跳过。
         */
        public Builder<C, P, T> shouldExecute(BiFunction<C, DependencyAccessor<C>, Boolean> shouldExecuteLogic) {
            this.shouldExecuteLogic = Objects.requireNonNull(shouldExecuteLogic);
            return this;
        }

        /**
         * 设置节点的执行逻辑。
         * @param executionLogic 一个函数，接收上下文和依赖访问器，返回 Mono<NodeResult>。
         *                       需要确保返回的 NodeResult 类型与 Builder 的 P 和 T 匹配。
         */
        public Builder<C, P, T> executionLogic(BiFunction<C, DependencyAccessor<C>, Mono<NodeResult<C, P, T>>> executionLogic) {
            this.executionLogic = Objects.requireNonNull(executionLogic);
            return this;
        }

        /**
         * 提供一个简单的执行逻辑，直接返回成功的 NodeResult (包含指定 payload)。
         * @param payload 要返回的 payload (可以为 null)
         */
        public Builder<C, P, T> returnsPayload(P payload) {
            this.executionLogic = (ctx, deps) -> Mono.just(NodeResult.success(ctx, payload, payloadType, eventType));
            return this;
        }

        /**
         * 提供一个简单的执行逻辑，直接返回失败的 NodeResult。
         * @param error 错误
         */
        public Builder<C, P, T> failsWith(Throwable error) {
            this.executionLogic = (ctx, deps) -> Mono.just(NodeResult.failure(ctx, error, payloadType, eventType));
            return this;
        }

        public Builder<C, P, T> retrySpec(Retry retrySpec) {
            this.retrySpec = retrySpec;
            return this;
        }

        public Builder<C, P, T> executionTimeout(Duration executionTimeout) {
            this.executionTimeout = executionTimeout;
            return this;
        }

        public TestDagNode<C, P, T> build() {
            if (this.executionLogic == null) {
                this.executionLogic = (ctx, deps) -> Mono.just(NodeResult.success(ctx, (P) null, payloadType, eventType));
            }
            return new TestDagNode<>(this);
        }
    }
}
