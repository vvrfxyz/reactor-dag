# Reactor DAG

[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/xyz.vvrf/reactor-dag.svg)](https://search.maven.org/search?q=g:xyz.vvrf%20AND%20a:reactor-dag)

基于Project Reactor的有向无环图（DAG）框架，支持构建复杂的反应式数据处理流程。

## 特性

- 基于Project Reactor，完全异步非阻塞的处理模型
- 内置节点依赖管理和拓扑排序
- 自动处理节点间数据流转
- 支持实时事件流，适用于长时间运行的任务
- 支持Spring框架和Spring Boot无缝集成
- 包含丰富的日志和可视化工具，方便调试和分析

## 快速开始

### Maven依赖

```xml
<dependency>
    <groupId>xyz.vvrf</groupId>
    <artifactId>reactor-dag</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 基本用法

1. 定义上下文类
```java
/**
 * 定义处理上下文，用于存储任务执行状态和中间结果
 */
public class ProcessingContext {
    private String input;
    private String result;
    // getters and setters...
}
```

2. 实现DAG节点
```java
/**
 * 输入验证节点
 */
public class ValidationNode implements DagNode<ProcessingContext, Boolean> {
    
    @Override
    public String getName() {
        return "validation";
    }
    
    @Override
    public List<DependencyDescriptor> getDependencies() {
        return Collections.emptyList(); // 无依赖
    }
    
    @Override
    public Class<Boolean> getPayloadType() {
        return Boolean.class;
    }
    
    @Override
    public Mono<NodeResult<ProcessingContext, Boolean>> execute(
            ProcessingContext context, 
            Map<String, NodeResult<ProcessingContext, ?>> dependencyResults) {
        
        boolean isValid = context.getInput() != null && !context.getInput().isEmpty();
        
        // 创建事件流
        Flux<Event<Boolean>> events = Flux.just(
            Event.of("validation.result", isValid ? "有效输入" : "无效输入"),
            Event.of("validation.complete", isValid)
        );
        
        return Mono.just(new NodeResult<>(context, events, Boolean.class));
    }
}

/**
 * 处理节点
 */
public class ProcessingNode implements DagNode<ProcessingContext, String> {
    
    @Override
    public String getName() {
        return "processing";
    }
    
    @Override
    public List<DependencyDescriptor> getDependencies() {
        return Collections.singletonList(
            new DependencyDescriptor("validation", Boolean.class)
        );
    }
    
    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }
    
    @Override
    public Mono<NodeResult<ProcessingContext, String>> execute(
            ProcessingContext context, 
            Map<String, NodeResult<ProcessingContext, ?>> dependencyResults) {
        
        // 检查依赖节点的结果
        NodeResult<ProcessingContext, Boolean> validationResult = 
            (NodeResult<ProcessingContext, Boolean>) dependencyResults.get("validation");
            
        if (!validationResult.getError().isPresent() && validationResult.getData()) {
            // 处理输入
            String result = context.getInput().toUpperCase();
            context.setResult(result);
            
            // 创建事件流
            Flux<Event<String>> events = Flux.just(
                Event.of("processing.start", "开始处理"),
                Event.of("processing.progress", "50%完成"),
                Event.of("processing.complete", result)
            ).delayElements(Duration.ofMillis(100)); // 模拟处理时间
            
            return Mono.just(new NodeResult<>(context, events, String.class));
        } else {
            // 验证失败，返回错误
            return Mono.just(new NodeResult<>(
                context, 
                Flux.empty(), 
                new IllegalArgumentException("无效输入"), 
                String.class
            ));
        }
    }
}
```

3. 定义DAG
```java
/**
 * 示例DAG定义
 */
public class ExampleDag extends AbstractDagDefinition<ProcessingContext> {
    
    public ExampleDag(List<DagNode<ProcessingContext, ?>> nodes) {
        super(ProcessingContext.class, nodes);
        initializeIfNeeded();
    }
}
```

4. 执行DAG
```java
// 创建节点执行器和DAG引擎
StandardNodeExecutor nodeExecutor = new StandardNodeExecutor(
    Duration.ofSeconds(30),  // 默认节点超时时间
    Duration.ofMinutes(3)    // 依赖流等待超时时间
);

StandardDagEngine dagEngine = new StandardDagEngine(
    nodeExecutor,
    Duration.ofMinutes(5)    // 缓存生存时间
);

// 创建处理上下文
ProcessingContext context = new ProcessingContext();
context.setInput("hello world");

// 创建DAG实例
List<DagNode<ProcessingContext, ?>> nodes = Arrays.asList(
    new ValidationNode(),
    new ProcessingNode()
);
ExampleDag exampleDag = new ExampleDag(nodes);

// 执行DAG并订阅事件流
String requestId = UUID.randomUUID().toString();
dagEngine.execute(context, requestId, exampleDag)
    .subscribe(event -> {
        System.out.println("事件类型: " + event.getEventType());
        System.out.println("事件数据: " + event.getData());
    }, error -> {
        System.err.println("处理错误: " + error.getMessage());
    }, () -> {
        System.out.println("处理完成!");
        System.out.println("结果: " + context.getResult());
    });
```

### Spring整合使用

1. 添加Spring配置

```java
@Configuration
@Import(DagFrameworkConfiguration.class)
public class AppConfig {
    
    @Bean
    public ValidationNode validationNode() {
        return new ValidationNode();
    }
    
    @Bean
    public ProcessingNode processingNode() {
        return new ProcessingNode();
    }
    
    @Bean
    public ExampleDag exampleDag(List<DagNode<ProcessingContext, ?>> nodes) {
        return new ExampleDag(nodes);
    }
}
```

2. 在服务中使用

```java
@Service
public class ProcessingService {
    
    @Autowired
    private SpringDagEngine dagEngine;
    
    @Autowired
    private ExampleDag exampleDag;
    
    public Flux<ServerSentEvent<?>> process(String input) {
        ProcessingContext context = new ProcessingContext();
        context.setInput(input);
        
        String requestId = UUID.randomUUID().toString();
        return dagEngine.execute(context, requestId, exampleDag);
    }
}
```

3. 在控制器中使用

```java
@RestController
public class ProcessingController {
    
    @Autowired
    private ProcessingService processingService;
    
    @GetMapping(value = "/process", produces = "text/event-stream")
    public Flux<ServerSentEvent<?>> process(@RequestParam String input) {
        return processingService.process(input);
    }
}
```

### Spring Boot自动配置

如果你使用Spring Boot，只需添加以下依赖：

```xml
<dependency>
    <groupId>xyz.vvrf</groupId>
    <artifactId>reactor-dag-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

然后在application.properties中配置:

```properties
# DAG框架配置
dag.node.default.timeout=30s
dag.dependency.stream.timeout=180s
dag.engine.cache.ttl=5m
```

## 高级功能

### 配置节点超时

```java
public class LongRunningNode implements DagNode<ProcessingContext, String> {
    
    @Override
    public Duration getExecutionTimeout() {
        return Duration.ofMinutes(5); // 覆盖默认超时时间
    }
    
    // 其他实现...
}
```

### 可视化DAG结构

框架在初始化时会自动生成DOT格式的图结构描述，你可以使用Graphviz等工具将其可视化:

```
[ProcessingContext] DAG 'ExampleDag' DOT格式图:
digraph DAG_ProcessingContext_ExampleDag {
  label="DAG Structure (ProcessingContext - ExampleDag)";
  rankdir=LR;
  node [shape=box, style="rounded,filled", fontname="Arial"];
  "validation" [label="validation\n(Boolean)\nValidationNode"];
  "processing" [label="processing\n(String)\nProcessingNode"];
  "validation" -> "processing";
}
```

## 设计理念

Reactor DAG框架设计用于解决现代应用中复杂的数据处理流程问题，特别是：

1. **模块化和可重用**：每个处理步骤被封装为一个独立的节点，可以在不同的DAG中重用。
2. **依赖管理**：框架自动处理节点间的依赖关系，确保按正确的顺序执行。
3. **反应式**：基于Project Reactor，全链路支持异步非阻塞模型。
4. **实时反馈**：通过事件流提供处理过程的实时反馈，适用于长时间运行的任务。
5. **弹性和容错**：内置超时处理、错误传播和恢复机制。

## 贡献

欢迎贡献代码、报告问题或提出改进建议！请查看[贡献指南](CONTRIBUTING.md)了解更多信息。

## 许可证

本项目采用Apache 2.0许可证，详情参见[LICENSE](LICENSE)文件。