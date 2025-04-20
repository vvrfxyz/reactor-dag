package xyz.vvrf.reactor.dag.example;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import xyz.vvrf.reactor.dag.example.dataParalleDag.DataParalleDag;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;
import xyz.vvrf.reactor.dag.spring.SpringDagEngine;

import java.util.UUID;

/**
 * DAG框架使用示例 - 一个简单的API服务
 */
@SpringBootApplication(exclude = {R2dbcAutoConfiguration.class}) // Exclude R2DBC auto-configuration
@RestController
@Slf4j
public class DagUsageExample {

    public static void main(String[] args) {
        SpringApplication.run(DagUsageExample.class, args);
    }

    @Autowired
    private SpringDagEngine dagEngine;

    @Autowired
    private DataParalleDag dataParalleDag;

    /**
     * 处理数据的API端点，以SSE方式返回处理过程
     */
//    @GetMapping(value = "/process", produces = "text/event-stream")
//    public Flux<ServerSentEvent<?>> processData(
//            @RequestParam(defaultValue = "100") int dataSize,
//            @RequestParam(defaultValue = "false") boolean includeEnrichment) {
//
//        // 创建处理上下文
//        ProcessingContext context = new ProcessingContext();
//        context.setDataSize(dataSize);
//        context.setIncludeEnrichment(includeEnrichment);
//
//        // 生成请求ID
//        String requestId = UUID.randomUUID().toString();
//
//        // 执行DAG并返回事件流
//        return dagEngine.execute(context, requestId, dataProcessingDag);
//    }

    @GetMapping(value = "/process-paralle", produces = "text/event-stream")
    public Flux<ServerSentEvent<?>> paralleData() {

        // 创建处理上下文
        ParalleContext context = new ParalleContext();

        // 生成请求ID
        String requestId = UUID.randomUUID().toString();

        // 执行DAG并返回事件流
        return dagEngine.execute(context, requestId, dataParalleDag);
    }
}