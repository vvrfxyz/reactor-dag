package xyz.vvrf.reactor.dag.example;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
// import org.springframework.web.bind.annotation.RequestParam; // 移除了未使用的导入
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.Event; // 引入 Event
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;
// 移除 SpringDagEngine 导入
import xyz.vvrf.reactor.dag.execution.DagEngine; // 引入核心 DagEngine
import xyz.vvrf.reactor.dag.spring.EventAdapter; // 引入 EventAdapter
import xyz.vvrf.reactor.dag.spring.boot.DagEngineProvider; // 引入 DagEngineProvider

import java.util.UUID;

/**
 * DAG框架使用示例 - 一个简单的 Spring Boot Web 服务。
 * 演示如何注入 DagEngineProvider 并执行 DAG。
 */
@SpringBootApplication(
        scanBasePackages = {"xyz.vvrf.reactor.dag.example", "xyz.vvrf.reactor.dag.spring.boot"}, // 确保能扫描到自动配置
        exclude = {R2dbcAutoConfiguration.class} // 排除 R2DBC 自动配置（如果不需要）
)
@RestController
@Slf4j
public class DagUsageExample {

    public static void main(String[] args) {
        SpringApplication.run(DagUsageExample.class, args);
    }

    // 注入 DagEngineProvider，而不是具体的引擎实例
    @Autowired
    private DagEngineProvider dagEngineProvider;

    // 注入预定义的 DAG Definition Bean
    @Autowired
    private DagDefinition<ParalleContext> dataParalleDag; // Bean 名称与方法名一致

    /**
     * 处理并行数据处理 DAG 的端点。
     * 返回 Server-Sent Events (SSE) 流。
     * @return SSE 事件流
     */
    @GetMapping(value = "/process-paralle", produces = "text/event-stream")
    public Flux<ServerSentEvent<?>> paralleData() {
        log.info("收到 /process-paralle 请求");

        // 1. 创建处理上下文
        ParalleContext context = new ParalleContext("初始输入数据"); // 可以设置初始值

        // 2. 生成请求 ID (可选，但推荐)
        String requestId = "req-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("请求 ID: {}", requestId);

        try {
            // 3. 从 Provider 获取特定上下文的核心 DagEngine
            log.debug("从 DagEngineProvider 获取 ParalleContext 的引擎...");
            DagEngine<ParalleContext> coreEngine = dagEngineProvider.getEngine(ParalleContext.class);
            log.debug("成功获取到引擎: {}", coreEngine.getClass().getSimpleName());

            // 4. 执行 DAG (使用核心引擎)
            log.info("开始执行 DAG: {}", dataParalleDag.getDagName());
            Flux<Event<?>> eventFlux = coreEngine.execute(context, dataParalleDag, requestId);

            // 5. 将核心 Event 流转换为 SSE 流并返回
            return EventAdapter.toServerSentEvents(eventFlux)
                    .doOnError(e -> log.error("[请求ID: {}] DAG 执行或 SSE 转换出错", requestId, e))
                    .doOnComplete(() -> log.info("[请求ID: {}] DAG 执行完成，SSE 流关闭", requestId));

        } catch (Exception e) {
            // 处理获取引擎或执行前的其他异常
            log.error("[请求ID: {}] 准备或执行 DAG 时发生错误", requestId, e);
            // 返回一个包含错误的 SSE 事件流
            return Flux.just(ServerSentEvent.<Object>builder()
                    .event("error")
                    .data("DAG 执行失败: " + e.getMessage())
                    .build());
        }
    }
}
