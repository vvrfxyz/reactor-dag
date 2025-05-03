package xyz.vvrf.reactor.dag.spring;

import lombok.extern.slf4j.Slf4j;
// 移除未使用的 Autowired
// import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.codec.ServerSentEvent;
// 移除未使用的 Service 注解 (如果此类不由 Spring 直接扫描创建，则不需要)
// import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.Event; // 正确导入 Event
import xyz.vvrf.reactor.dag.impl.StandardDagEngine;
import xyz.vvrf.reactor.dag.impl.StandardNodeExecutor;
import xyz.vvrf.reactor.dag.spring.boot.DagFrameworkProperties; // 导入属性类

import java.util.Objects;

/**
 * Spring集成的DAG执行引擎 - 提供与Spring框架的无缝集成并发送ServerSentEvent流
 *
 * @author ruifeng.wen (modified)
 */
@Slf4j
public class SpringDagEngine {

    private final StandardDagEngine dagEngine; // 底层的标准引擎

    /**
     * 创建SpringDagEngine实例。
     * 注入配置好的 StandardNodeExecutor 和 DagFrameworkProperties。
     * 使用这些属性创建内部的 StandardDagEngine 实例。
     *
     * @param nodeExecutor 配置好的 StandardNodeExecutor Bean。
     * @param properties   DAG 框架的配置属性。
     */
    public SpringDagEngine(
            StandardNodeExecutor nodeExecutor,
            DagFrameworkProperties properties // 注入属性对象
    ) {
        Objects.requireNonNull(nodeExecutor, "StandardNodeExecutor cannot be null");
        Objects.requireNonNull(properties, "DagFrameworkProperties cannot be null");

        // 使用注入的执行器和从 DagFrameworkProperties 对象读取的属性
        // 来创建内部的 StandardDagEngine 实例。
        // 注意：StandardDagEngine 构造函数不再需要 cacheTtl
        this.dagEngine = new StandardDagEngine(
                nodeExecutor,
                properties.getEngine().getConcurrencyLevel() // 只传递并发级别
        );

        log.info("SpringDagEngine 初始化完成，使用的配置: {}", properties); // 记录使用的配置
    }

    /**
     * 执行指定 DAG 定义并返回合并后的SpringServerSentEvent事件流。
     * 将执行委托给内部的 StandardDagEngine 并转换事件。
     *
     * @param <C>            上下文类型
     * @param initialContext 初始上下文对象
     * @param requestId      请求的唯一标识符，用于日志和追踪
     * @param dagDefinition  要执行的 DAG 的定义
     * @return 合并所有节点事件流的 Flux<ServerSentEvent<?>>
     */
    public <C> Flux<ServerSentEvent<?>> execute(
            final C initialContext,
            final String requestId,
            final DagDefinition<C> dagDefinition
    ) {
        // 委托给标准引擎执行
        Flux<Event<?>> coreEventFlux = dagEngine.execute(initialContext, requestId, dagDefinition);

        // 将核心 Event 流转换为 ServerSentEvent 流
        return EventAdapter.toServerSentEvents(coreEventFlux);
    }

}
