package xyz.vvrf.reactor.dag.spring;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.impl.StandardDagEngine;
import xyz.vvrf.reactor.dag.impl.StandardNodeExecutor;

import java.time.Duration;

/**
 * Spring集成的DAG执行引擎 - 提供与Spring框架的无缝集成并发送ServerSentEvent流
 *
 * @author ruifeng.wen
 */
@Slf4j
@Service
public class SpringDagEngine {

    private final StandardDagEngine dagEngine;

    /**
     * 创建SpringDagEngine实例
     *
     * @param nodeExecutor 节点执行器
     * @param cacheTtl 缓存生存时间
     */
    @Autowired
    public SpringDagEngine(
            StandardNodeExecutor nodeExecutor,
            @Value("${dag.engine.cache.ttl:5m}") Duration cacheTtl) {
        this.dagEngine = new StandardDagEngine(nodeExecutor, cacheTtl);
        log.info("SpringDagEngine 初始化完成，缓存TTL: {}", cacheTtl);
    }

    /**
     * 执行指定 DAG 定义并返回合并后的SpringServerSentEvent事件流。
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
        Flux<ServerSentEvent<?>> mainFlux = dagEngine.execute(initialContext, requestId, dagDefinition)
                .map(this::convertToServerSentEvent);

        return mainFlux;
    }

    /**
     * 将通用Event转换为Spring的ServerSentEvent
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private ServerSentEvent<?> convertToServerSentEvent(xyz.vvrf.reactor.dag.core.Event<?> event) {
        return ServerSentEvent.builder()
                .event(event.getEventType())
                .id(event.getId())
                .data(event.getData())
                .comment(event.getComment())
                .build();
    }

}