// file: spring/SpringDagEngine.java
package xyz.vvrf.reactor.dag.spring;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.execution.DagEngine; // 依赖核心引擎接口

import java.util.Objects;

/**
 * Spring 集成的 DAG 执行引擎包装器。
 * 包装一个核心 {@link DagEngine<C>} 实例，并提供将核心 {@link Event} 流
 * 转换为 Spring {@link ServerSentEvent} 流的功能。
 *
 * 此类本身是泛型的，需要由用户针对具体上下文类型 C 进行实例化和配置，
 * 通常注入由 {@link xyz.vvrf.reactor.dag.spring.boot.DagEngineProvider} 提供的核心引擎。
 *
 * @param <C> 上下文类型
 * @author Refactored
 */
@Slf4j
public class SpringDagEngine<C> { // 类本身泛型化

    private final DagEngine<C> coreDagEngine; // 依赖核心引擎接口

    /**
     * 创建 SpringDagEngine 实例。
     *
     * @param coreDagEngine 配置好的、特定于上下文类型 C 的核心 DagEngine 实例。
     *                      通常通过 {@code DagEngineProvider.getEngine(Context.class)} 获取。
     */
    public SpringDagEngine(DagEngine<C> coreDagEngine) {
        this.coreDagEngine = Objects.requireNonNull(coreDagEngine, "Core DagEngine cannot be null");
        // 注意：这里无法直接获取 Context Type，除非 DagEngine 接口提供
        log.info("SpringDagEngine wrapper initialized, wrapping core engine: {}", coreDagEngine.getClass().getSimpleName());
    }

    /**
     * 执行指定 DAG 定义并返回合并后的 Spring ServerSentEvent 事件流。
     * 将执行委托给内部的核心 DagEngine 并使用 {@link EventAdapter} 转换事件。
     *
     * @param initialContext 初始上下文对象。
     * @param dagDefinition  要执行的 DAG 的定义。
     * @param requestId      请求的唯一标识符，用于日志和追踪 (如果为 null 或空，核心引擎会生成默认值)。
     * @return 合并所有成功节点事件流并转换为 SSE 的 Flux<ServerSentEvent<?>>。
     */
    public Flux<ServerSentEvent<?>> executeAsSse(
            final C initialContext,
            final DagDefinition<C> dagDefinition,
            final String requestId
    ) {
        log.debug("Executing DAG '{}' via SpringDagEngine wrapper for SSE output. RequestId: {}",
                dagDefinition.getDagName(), requestId);

        // 1. 委托给核心引擎执行，获取核心 Event 流
        Flux<Event<?>> coreEventFlux = coreDagEngine.execute(initialContext, dagDefinition, requestId);

        // 2. 将核心 Event 流转换为 ServerSentEvent 流
        return EventAdapter.toServerSentEvents(coreEventFlux)
                .doOnSubscribe(s -> log.debug("[RequestId: {}][DAG: '{}'] SSE stream subscribed.", requestId, dagDefinition.getDagName()))
                .doOnError(e -> log.error("[RequestId: {}][DAG: '{}'] Error occurred in SSE stream.", requestId, dagDefinition.getDagName(), e))
                .doOnComplete(() -> log.debug("[RequestId: {}][DAG: '{}'] SSE stream completed.", requestId, dagDefinition.getDagName()));
    }

    /**
     * 提供对底层核心引擎的访问（如果需要直接使用核心引擎功能）。
     * @return 底层的 DagEngine<C> 实例。
     */
    public DagEngine<C> getCoreDagEngine() {
        return coreDagEngine;
    }
}
