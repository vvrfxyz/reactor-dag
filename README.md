# Reactor DAG

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Reactor DAG** 是一个基于 [Project Reactor](https://projectreactor.io/) 构建的轻量级、高性能的反应式有向无环图 (DAG) 执行框架。它允许您定义具有复杂依赖关系的异步任务，并以非阻塞、高效的方式执行它们，特别适用于需要编排多个异步操作（如微服务调用、数据处理步骤）的场景。

该框架提供了：

*   **反应式核心**: 完全基于 Project Reactor 构建，充分利用其强大的异步和背压能力。
*   **类型安全**: 利用 Java 泛型确保节点间数据传递的类型安全。
*   **灵活的依赖定义**: 支持节点自身定义依赖，也支持通过 `ChainBuilder` 进行外部、链式的依赖配置，覆盖节点内部定义。
*   **Spring Boot 集成**: 提供自动配置，简化在 Spring Boot 应用中的使用。
*   **事件流处理**: 节点可以产生事件流 (`Flux<Event<?>>`)，引擎负责合并这些流，方便进行 Server-Sent Events (SSE) 等场景。
*   **可配置性**: 提供超时、缓存、并发度等配置选项。
*   **可观测性**: 内置日志记录，包括 DAG 结构和执行顺序的可视化输出 (DOT 格式)。

---

## 目录

*   [快速入门](#快速入门)
    *   [添加依赖](#添加依赖)
    *   [基本用法 (非 Spring)](#基本用法-非-spring)
    *   [Spring Boot 集成用法](#spring-boot-集成用法)
*   [核心概念](#核心概念)
*   [API 参考](#api-参考)
*   [配置选项](#配置选项)
*   [高级特性](#高级特性)
    *   [显式依赖覆盖 (ChainBuilder)](#显式依赖覆盖-chainbuilder)
    *   [错误处理](#错误处理)
    *   [缓存](#缓存)
    *   [并发与调度](#并发与调度)
    *   [日志记录与可视化](#日志记录与可视化)
*   [贡献](#贡献)
*   [许可证](#许可证)

---

## 快速入门

### 添加依赖

**Maven:**

```xml
<dependency>
    <groupId>xyz.vvrf</groupId>
    <artifactId>reactor-dag</artifactId>
    <version>1.0.0</version> <!-- 使用最新版本 -->
</dependency>

<!-- 如果使用 Spring Boot 集成 -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-webflux</artifactId>
    <!-- 使用与您的 Spring Boot 版本兼容的版本 -->
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-validation</artifactId>
    <!-- 使用与您的 Spring Boot 版本兼容的版本 -->
</dependency>
```

**Gradle:**

```gradle
implementation 'xyz.vvrf:reactor-dag:1.0.0' // 使用最新版本

// 如果使用 Spring Boot 集成
implementation 'org.springframework.boot:spring-boot-starter-webflux'
implementation 'org.springframework.boot:spring-boot-starter-validation'
```

### 基本用法 (非 Spring)

1.  **定义上下文类型**:
    ```java
    // 定义一个简单的上下文类
    public class MyContext {
        private String inputData;
        private Map<String, Object> results = new ConcurrentHashMap<>();
        // Getters and Setters...
    }
    ```

2.  **创建 DAG 节点**: 实现 `DagNode<C, P, T>` 接口。
    ```java
    // 节点 A: 接收 MyContext，输出 String，不产生特定事件 (Void)
    @Slf4j
    public class NodeA implements DagNode<MyContext, String, Void> {
        @Override
        public Class<String> getPayloadType() { return String.class; }
        @Override
        public Class<Void> getEventType() { return Void.class; } // 无事件数据
    
        @Override
        public Mono<NodeResult<MyContext, String, Void>> execute(MyContext context, DependencyAccessor<MyContext> dependencies) {
            log.info("Executing NodeA...");
            String result = "Processed: " + context.getInputData();
            // 假设 NodeA 是起点，没有依赖
            // 创建成功的 NodeResult，只包含 Payload
            return Mono.just(NodeResult.success(context, result, this))
                       .delayElement(Duration.ofMillis(100)); // 模拟异步操作
        }
        // 默认无依赖: getDependencies() 返回空列表
    }
    
    // 节点 B: 接收 MyContext，输出 Integer，产生 String 类型事件
    @Slf4j
    public class NodeB implements DagNode<MyContext, Integer, String> {
        @Override
        public Class<Integer> getPayloadType() { return Integer.class; }
        @Override
        public Class<String> getEventType() { return String.class; }
    
        // 节点 B 内部定义依赖 NodeA
        @Override
        public List<DependencyDescriptor> getDependencies() {
            return Collections.singletonList(
                new DependencyDescriptor(NodeA.class.getSimpleName(), String.class) // 依赖 NodeA 的 String 输出
            );
        }
    
        @Override
        public Mono<NodeResult<MyContext, Integer, String>> execute(MyContext context, DependencyAccessor<MyContext> dependencies) {
            log.info("Executing NodeB...");
            // 安全地获取 NodeA 的结果
            return dependencies.getPayload(NodeA.class.getSimpleName(), String.class)
                .map(nodeAResult -> {
                    int result = nodeAResult.length(); // 计算长度作为结果
                    context.getResults().put("nodeB_result", result);
    
                    // 创建事件流
                    Flux<Event<String>> eventFlux = Flux.just(
                        Event.of("LengthCalculated", "NodeB calculated length: " + result),
                        Event.of("ProcessingStep", "NodeB processing finished")
                    ).delayElements(Duration.ofMillis(50)); // 模拟事件产生
    
                    // 创建成功的 NodeResult，包含 Payload 和事件流
                    return NodeResult.success(context, result, eventFlux, this);
                })
                .map(Mono::just) // 将 Optional<NodeResult> 转为 Mono<NodeResult>
                .orElseGet(() -> {
                    log.error("NodeB could not get dependency from NodeA!");
                    // 创建失败的 NodeResult
                    return Mono.just(NodeResult.failure(context, new RuntimeException("Missing dependency: NodeA"), this));
                });
        }
    }
    ```

3.  **定义 DAG 结构**: 继承 `AbstractDagDefinition<C>`。
    ```java
    public class MySimpleDag extends AbstractDagDefinition<MyContext> {
    
        // 通常通过构造函数注入节点实例
        public MySimpleDag(List<DagNode<MyContext, ?, ?>> nodes) {
            super(MyContext.class, nodes);
            // 在这里可以添加显式依赖，或者在外部使用 ChainBuilder
        }
    
        // 可选：覆盖 getDagName() 提供更友好的名称
        @Override
        public String getDagName() {
            return "MySimpleProcessingDag";
        }
    }
    ```

4.  **(可选) 使用 ChainBuilder 定义依赖**:
    ```java
    // 假设 NodeC 依赖 NodeA 和 NodeB
    @Slf4j
    public class NodeC implements DagNode<MyContext, Boolean, Void> {
        @Override public Class<Boolean> getPayloadType() { return Boolean.class; }
        @Override public Class<Void> getEventType() { return Void.class; }
        // NodeC 自身不定义 getDependencies()
    
        @Override
        public Mono<NodeResult<MyContext, Boolean, Void>> execute(MyContext context, DependencyAccessor<MyContext> dependencies) {
            log.info("Executing NodeC...");
            boolean success = dependencies.isSuccess(NodeA.class.getSimpleName()) &&
                              dependencies.isSuccess(NodeB.class.getSimpleName());
            // ... 获取 NodeA 和 NodeB 的 payload ...
            return Mono.just(NodeResult.success(context, success, this));
        }
    }
    
    // ... 在创建 MySimpleDag 实例后 ...
    NodeA nodeA = new NodeA();
    NodeB nodeB = new NodeB(); // NodeB 内部定义了对 A 的依赖
    NodeC nodeC = new NodeC();
    List<DagNode<MyContext, ?, ?>> allNodes = Arrays.asList(nodeA, nodeB, nodeC);
    
    MySimpleDag dagDefinition = new MySimpleDag(allNodes);
    
    // 使用 ChainBuilder 定义 NodeC 的依赖，覆盖其内部定义（如果存在）
    // 注意：ChainBuilder 需要 AbstractDagDefinition 实例
    ChainBuilder<MyContext> builder = new ChainBuilder<>(dagDefinition);
    builder.node(NodeC.class, NodeA.class, NodeB.class); // NodeC 依赖 NodeA 和 NodeB
    
    // 将 Builder 构建的显式依赖添加到 Definition 中
    dagDefinition.addExplicitDependencies(builder.build());
    
    // !!! 重要：在配置完所有显式依赖后，必须初始化 DAG 定义 !!!
    dagDefinition.initialize(); // 或 dagDefinition.initializeIfNeeded();
    ```
    *或者使用链式语法:*
    ```java
    // ... 创建 dagDefinition 实例 ...
    ChainBuilder<MyContext> builder = new ChainBuilder<>(dagDefinition);
    builder.startLinear(NodeA.class) // A 是起点
           .then(NodeB.class)      // B 依赖 A
           .node(NodeC.class, NodeA.class, NodeB.class); // C 依赖 A 和 B (独立于线性链)
    
    dagDefinition.addExplicitDependencies(builder.build());
    dagDefinition.initialize();
    ```

5.  **创建执行器和引擎**:
    ```java
    // 配置执行器
    Duration defaultTimeout = Duration.ofSeconds(10);
    StandardNodeExecutor nodeExecutor = new StandardNodeExecutor(defaultTimeout);
    
    // 配置引擎
    Duration cacheTtl = Duration.ofMinutes(1);
    int concurrency = 4; // 并发执行节点的数量
    StandardDagEngine dagEngine = new StandardDagEngine(nodeExecutor, cacheTtl, concurrency);
    ```

6.  **执行 DAG**:
    ```java
    MyContext initialContext = new MyContext();
    initialContext.setInputData("hello world");
    String requestId = "req-123"; // 用于追踪
    
    // 确保 dagDefinition 已经初始化！
    if (!dagDefinition.isInitialized()) {
       dagDefinition.initialize();
    }
    
    Flux<Event<?>> eventStream = dagEngine.execute(initialContext, requestId, dagDefinition);
    
    // 处理事件流
    eventStream.subscribe(
        event -> System.out.println("Received Event: " + event),
        error -> System.err.println("DAG Execution Error: " + error),
        () -> System.out.println("DAG Execution Completed.")
    );
    
    // 在实际应用中，需要阻塞等待或以其他方式管理异步流的完成
    // StepVerifier.create(eventStream).expectNextCount(2).verifyComplete(); // 在测试中
    Thread.sleep(2000); // 简单示例等待
    ```

### Spring Boot 集成用法

1.  **确保依赖**: 包含 `reactor-dag`, `spring-boot-starter-webflux`, `spring-boot-starter-validation`。
2.  **配置**: 在 `application.properties` 或 `application.yml` 中配置框架参数。
    ```properties
    # application.properties
    dag.node.default-timeout=PT15S # 节点默认超时 15 秒
    dag.engine.cache-ttl=PT2M     # 引擎缓存 TTL 2 分钟
    dag.engine.concurrency-level=8 # 引擎并发度 8
    ```
    ```yaml
    # application.yml
    dag:
      node:
        default-timeout: PT15S
      engine:
        cache-ttl: PT2M
        concurrency-level: 8
    ```
3.  **定义节点**: 将 `DagNode` 实现声明为 Spring Bean (`@Component`, `@Service` 等)。
    ```java
    @Component // 让 Spring 管理 NodeA
    public class NodeA implements DagNode<MyContext, String, Void> { /* ... */ }
    
    @Component // 让 Spring 管理 NodeB
    public class NodeB implements DagNode<MyContext, Integer, String> { /* ... */ }
    
    @Component // 让 Spring 管理 NodeC
    public class NodeC implements DagNode<MyContext, Boolean, Void> { /* ... */ }
    ```
4.  **定义 DAG Definition**: 将 `AbstractDagDefinition` 子类声明为 Spring Bean，并通过构造函数注入所有 `DagNode<MyContext, ?, ?>` 类型的 Bean。
    ```java
    @Configuration // 或 @Component
    public class MySimpleDag extends AbstractDagDefinition<MyContext> {
    
        private final ChainBuilder<MyContext> builder;
    
        @Autowired // 注入所有 MyContext 相关的节点 Bean
        public MySimpleDag(List<DagNode<MyContext, ?, ?>> nodes) {
            super(MyContext.class, nodes);
    
            // 在构造函数中使用 ChainBuilder 配置显式依赖
            this.builder = new ChainBuilder<>(this); // 'this' 就是 AbstractDagDefinition
            configureDependencies();
    
            // 将构建好的依赖添加到 Definition
            addExplicitDependencies(this.builder.build());
    
            // !!! 重要：在构造函数或 @PostConstruct 中初始化 !!!
            initialize(); // 确保在 Bean 创建完成后初始化
        }
    
        private void configureDependencies() {
            // 使用类名查找节点（假设节点类名是唯一的）
            // 注意：如果节点覆盖了 getName() 返回非类名，Builder 需要调整或使用字符串名称
            builder.startLinear(NodeA.class)
                   .then(NodeB.class)
                   .node(NodeC.class, NodeA.class, NodeB.class);
            // 或者 builder.node(NodeA.class); builder.node(NodeB.class, NodeA.class); ...
        }
    
        @Override
        public String getDagName() {
            return "MySpringBootDag";
        }
    
        // 可选: 使用 @PostConstruct 替代在构造函数末尾调用 initialize()
        // @PostConstruct
        // public void init() {
        //     initializeIfNeeded();
        // }
    }
    ```
5.  **注入并使用引擎**: 自动配置会提供 `SpringDagEngine` Bean。
    ```java
    @RestController
    @RequestMapping("/process")
    public class ProcessingController {
    
        @Autowired
        private SpringDagEngine springDagEngine;
    
        @Autowired // 注入特定上下文的 DAG 定义 Bean
        private MySimpleDag mySimpleDagDefinition;
    
        @GetMapping(value = "/start", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
        public Flux<ServerSentEvent<?>> startProcessing(@RequestParam String data) {
            MyContext context = new MyContext();
            context.setInputData(data);
            String requestId = UUID.randomUUID().toString().substring(0, 8);
    
            // SpringDagEngine 返回 Flux<ServerSentEvent<?>>
            return springDagEngine.execute(context, requestId, mySimpleDagDefinition);
        }
    }
    ```

---

## 核心概念

*   **DAG (Directed Acyclic Graph - 有向无环图)**:
    *   表示一组任务（节点）及其依赖关系。
    *   "有向" 指依赖关系是单向的（例如，A 依赖 B，不意味着 B 依赖 A）。
    *   "无环" 指依赖关系中不能存在循环（例如，A 依赖 B，B 依赖 C，C 不能再依赖 A）。
    *   `DagDefinition<C>` 接口及其抽象实现 `AbstractDagDefinition<C>` 负责定义和管理 DAG 的结构、节点和执行顺序。

*   **Node (节点 - `DagNode<C, P, T>`)**:
    *   DAG 中的基本执行单元，代表一个异步任务。
    *   泛型参数:
        *   `C`: 节点执行所需的上下文 (Context) 类型。
        *   `P`: 节点成功执行后产生的主要结果 (Payload) 类型。
        *   `T`: 节点执行过程中可能产生的事件 (Event) 的数据类型。
    *   核心方法 `execute(C context, DependencyAccessor<C> dependencies)` 执行节点逻辑，返回 `Mono<NodeResult<C, P, T>>`。
    *   可以通过 `getDependencies()` 定义其依赖的节点（名称和期望的 Payload 类型）。

*   **Result (结果 - `NodeResult<C, P, T>`)**:
    *   封装 `DagNode` 执行完成后的结果。
    *   包含：
        *   执行时的上下文 (`C`)。
        *   可选的 Payload (`Optional<P>`)。
        *   事件流 (`Flux<Event<T>>`)。
        *   可选的错误信息 (`Optional<Throwable>`)。
        *   实际的 Payload 类型和 Event 类型 (`Class<P>`, `Class<T>`)。
    *   通过静态工厂方法 `NodeResult.success(...)` 和 `NodeResult.failure(...)` 创建。

*   **Event (事件 - `Event<T>`)**:
    *   节点在执行过程中可以产生的离散事件。
    *   包含事件类型 (`String`)、唯一 ID (`String`)、事件数据 (`T`) 和可选注释 (`String`)。
    *   `StandardDagEngine` 会合并所有节点产生的事件流。
    *   `SpringDagEngine` 会将 `Event<?>` 转换为 `ServerSentEvent<?>`。

*   **Context (上下文 - `C`)**:
    *   一个普通的 Java 对象，用于在 DAG 执行的不同节点间传递共享状态或数据。
    *   其类型由 `DagDefinition` 和 `DagNode` 的泛型参数 `C` 指定。
    *   引擎在启动执行时传入初始上下文，并在节点间传递（`NodeResult` 中包含上下文）。

*   **Builder (构建器 - `ChainBuilder<C>`)**:
    *   一个流畅 API (Fluent API) 工具，用于以编程方式定义节点间的依赖关系。
    *   允许在外部（例如 `DagDefinition` 的构造函数或配置类中）集中定义依赖，覆盖节点自身的 `getDependencies()`。
    *   与 `AbstractDagDefinition` 的 `addExplicitDependencies()` 方法配合使用。
    *   提供 `node()`, `startWith()`, `startLinear()`, `then()` 等方法简化 DAG 结构定义。

*   **Engine (引擎 - `StandardDagEngine`, `SpringDagEngine`)**:
    *   负责接收 `DagDefinition` 和初始上下文，并协调整个 DAG 的执行。
    *   `StandardDagEngine`: 核心执行引擎。
        *   获取拓扑排序后的执行顺序。
        *   使用 `StandardNodeExecutor` 执行每个节点。
        *   管理请求级别的节点结果缓存。
        *   合并所有节点的 `Flux<Event<?>>` 事件流。
        *   控制并发度。
    *   `SpringDagEngine`: Spring 集成版本。
        *   包装 `StandardDagEngine`。
        *   通过 Spring Boot 自动配置进行配置。
        *   其 `execute` 方法返回 `Flux<ServerSentEvent<?>>`，方便与 WebFlux 控制器集成。

*   **Executor (执行器 - `StandardNodeExecutor`)**:
    *   负责执行 *单个* `DagNode`。
    *   处理：
        *   解析节点的有效依赖 (`DagDefinition.getEffectiveDependencies`)。
        *   递归调用自身获取依赖节点的 `Mono<NodeResult>` (利用缓存)。
        *   创建 `DependencyAccessor` 传递给节点的 `execute` 方法。
        *   在指定的 `Scheduler` 上执行节点逻辑。
        *   处理节点超时。
        *   捕获节点执行错误并包装为 `NodeResult.failure`。
        *   利用请求级缓存 (`Cache`) 避免重复计算。

*   **DependencyDescriptor (依赖描述符)**:
    *   一个简单的数据对象，用于描述一个节点对另一个节点的依赖。
    *   包含依赖节点的名称 (`String`) 和当前节点期望从依赖节点获取的 Payload 类型 (`Class<?>`)。

*   **DependencyAccessor (依赖访问器)**:
    *   传递给 `DagNode.execute()` 方法的接口，取代了直接传递原始结果 Map。
    *   提供类型安全、便捷的方法来访问依赖节点的执行结果，如 `getPayload(name, type)`, `getEvents(name)`, `isSuccess(name)` 等。

---

## API 参考

以下是核心公共 API 的简要说明：

*   **`xyz.vvrf.reactor.dag.core.DagDefinition<C>`**:
    *   `getEffectiveDependencies(String nodeName)`: 获取节点的最终生效依赖（优先外部配置）。
    *   `getNode(String nodeName, Class<P> payloadType)`: 按名称和 Payload 类型获取节点。
    *   `getNodeAnyType(String nodeName)`: 按名称获取节点（不关心类型）。
    *   `getAllNodes()`: 获取所有已注册节点。
    *   `getExecutionOrder()`: 获取计算好的拓扑执行顺序（节点名称列表）。
    *   `initialize()` / `initializeIfNeeded()`: 验证 DAG 并计算执行顺序（必须调用）。
    *   `isInitialized()`: 检查是否已初始化。
    *   `getContextType()`: 返回 DAG 关联的上下文类型。

*   **`xyz.vvrf.reactor.dag.core.DagNode<C, P, T>`**:
    *   `getDependencies()`: （可选）定义节点自身的依赖。
    *   `getPayloadType()`: 返回节点输出的 Payload 类型。
    *   `getEventType()`: 返回节点产生的事件数据类型。
    *   `execute(C context, DependencyAccessor<C> dependencies)`: 实现节点执行逻辑。
    *   `getExecutionTimeout()`: （可选）定义节点特定的超时时间。

*   **`xyz.vvrf.reactor.dag.core.NodeResult<C, P, T>`**:
    *   `success(...)` / `failure(...)`: 创建结果实例的工厂方法。
    *   `getPayload()`: 获取 `Optional<P>` 结果。
    *   `getEvents()`: 获取 `Flux<Event<T>>` 事件流。
    *   `getError()`: 获取 `Optional<Throwable>` 错误。
    *   `isSuccess()` / `isFailure()`: 检查执行状态。
    *   `getPayloadType()` / `getEventType()`: 获取结果关联的类型。

*   **`xyz.vvrf.reactor.dag.core.Event<T>`**:
    *   `of(...)` / `builder()`: 创建事件实例。
    *   `getEventType()`, `getId()`, `getData()`, `getComment()`: 获取事件属性。

*   **`xyz.vvrf.reactor.dag.core.DependencyAccessor<C>`**:
    *   `getPayload(String dependencyName, Class<DepP> expectedType)`: 安全获取依赖 Payload。
    *   `getEvents(String dependencyName)`: 获取依赖的事件流。
    *   `getResult(String dependencyName)`: 获取完整的依赖 `NodeResult`。
    *   `isSuccess(String dependencyName)`: 检查依赖是否成功。
    *   `contains(String dependencyName)`: 检查是否存在该依赖的结果。

*   **`xyz.vvrf.reactor.dag.builder.ChainBuilder<C>`**:
    *   `node(Class<? extends DagNode> nodeClass, Class<?>... dependencyClasses)`: 定义节点及其依赖。
    *   `startWith(Class<?> nodeClass)`: 定义无依赖的起始节点。
    *   `startLinear(Class<?> nodeClass)` / `then(Class<?> nodeClass)`: 定义线性依赖链。
    *   `build()`: 构建最终的显式依赖映射。

*   **`xyz.vvrf.reactor.dag.impl.StandardDagEngine`**:
    *   `execute(C initialContext, String requestId, DagDefinition<C> dagDefinition)`: 执行 DAG，返回 `Flux<Event<?>>`。

*   **`xyz.vvrf.reactor.dag.spring.SpringDagEngine`**:
    *   `execute(C initialContext, String requestId, DagDefinition<C> dagDefinition)`: 执行 DAG，返回 `Flux<ServerSentEvent<?>>`。

*   **`xyz.vvrf.reactor.dag.impl.StandardNodeExecutor`**:
    *   (通常不直接使用，由引擎内部调用) `getNodeExecutionMono(...)`: 获取或创建节点的执行 Mono。

*   **`xyz.vvrf.reactor.dag.impl.AbstractDagDefinition<C>`**:
    *   (作为基类使用) `addExplicitDependencies(String nodeName, List<DependencyDescriptor> dependencies)` / `addExplicitDependencies(Map<String, List<DependencyDescriptor>> map)`: 添加外部定义的依赖。

---

## 配置选项

当使用 Spring Boot 集成时 (`DagFrameworkAutoConfiguration`)，可以通过 `application.properties` 或 `application.yml` 文件配置以下属性：

| 属性 Key                       | 描述                                                         | 类型                 | 默认值                                       |
| :----------------------------- | :----------------------------------------------------------- | :------------------- | :------------------------------------------- |
| `dag.node.default-timeout`     | 节点的默认执行超时时间。如果节点自身未定义超时，则使用此值。 | `java.time.Duration` | `PT30S` (30 秒)                              |
| `dag.engine.cache-ttl`         | 请求级别节点结果缓存 (`Mono<NodeResult>`) 的生存时间 (Time-To-Live)。 | `java.time.Duration` | `PT5M` (5 分钟)                              |
| `dag.engine.concurrency-level` | `StandardDagEngine` 在处理（订阅）节点执行 Mono 时的并发度 (`flatMap` 操作)。 | `int`                | `Runtime.getRuntime().availableProcessors()` |

**示例 (application.yml):**

```yaml
dag:
  node:
    default-timeout: PT10S # 10 秒超时
  engine:
    cache-ttl: PT1M      # 1 分钟缓存
    concurrency-level: 16 # 并发度 16
```

---

## 高级特性

### 显式依赖覆盖 (ChainBuilder)

`ChainBuilder` 提供了一种强大的方式来在外部定义 DAG 结构，特别适用于以下场景：

*   **复用节点**: 同一个 `DagNode` 实现可能在不同的 DAG 中有不同的依赖关系。
*   **集中管理**: 将 DAG 结构定义与节点实现逻辑分离。
*   **动态构建**: 基于配置或其他条件动态构建 DAG 依赖。

使用 `ChainBuilder` 定义的依赖会覆盖 `DagNode` 内部通过 `getDependencies()` 定义的依赖。

```java
// 在 AbstractDagDefinition 子类的构造函数或配置类中
MyDagDefinition extends AbstractDagDefinition<MyContext> {
    public MyDagDefinition(List<DagNode<MyContext, ?, ?>> nodes) {
        super(MyContext.class, nodes);
        ChainBuilder<MyContext> builder = new ChainBuilder<>(this);
        // ... 使用 builder.node(), builder.then() 等定义依赖 ...
        Map<String, List<DependencyDescriptor>> explicitDeps = builder.build();
        addExplicitDependencies(explicitDeps); // 添加显式依赖
        initialize(); // 初始化
    }
}
```

### 错误处理

*   **节点级别**: `DagNode` 的 `execute` 方法可以返回 `NodeResult.failure(context, error, this)` 来表示节点执行失败。错误信息会被封装在 `NodeResult` 中。
*   **执行器级别**: `StandardNodeExecutor` 会捕获节点执行过程中的异常（包括超时 `TimeoutException`），并将其转换为 `NodeResult.failure`。
*   **依赖访问**: `DependencyAccessor` 允许下游节点检查上游依赖是否成功 (`isSuccess()`)，并安全地获取结果 (`getPayload()`)。如果依赖失败，`getPayload()` 返回 `Optional.empty()`。
*   **引擎级别**: `StandardDagEngine` 会合并所有节点的事件流。如果某个节点的执行 Mono 本身失败（例如，依赖解析失败），其事件流通常会被 `onErrorResume` 处理为空流，避免中断整个 DAG 的事件合并。最终的 `Flux<Event<?>>` 或 `Flux<ServerSentEvent<?>>` 可能会因为合并过程中的错误而终止（可以通过 `.onErrorResume()` 等操作符处理）。

### 缓存

`StandardDagEngine` 和 `StandardNodeExecutor` 协同工作，实现了请求级别的缓存：

*   引擎为每次 `execute` 调用创建一个 `Cache<String, Mono<? extends NodeResult<C, ?, ?>>>` 实例。
*   执行器在获取节点执行 Mono 时，会先检查缓存 (`cacheKey = nodeName + "#" + payloadTypeName`)。
*   如果命中，直接返回缓存的 `Mono`。
*   如果未命中，创建新的执行 `Mono`，并使用 `.cache()` 操作符确保其只执行一次，然后放入缓存。
*   缓存的 TTL 由 `dag.engine.cache-ttl` 配置。
*   DAG 执行完成后（成功、失败或取消），引擎会清理当前请求的缓存。

这可以显著提高性能，尤其是在扇出（一个节点被多个其他节点依赖）场景下，避免了同一节点在同一次请求中被重复执行。

### 并发与调度

*   **节点执行**: `StandardNodeExecutor` 默认使用 `Schedulers.boundedElastic()` 来执行 `DagNode.execute()` 逻辑。可以通过构造函数注入自定义的 `Scheduler`。
*   **引擎并发**: `StandardDagEngine` 使用 `Flux.fromIterable(nodeNames).flatMap(..., concurrencyLevel)` 来并发订阅和处理拓扑排序后的节点执行 Mono。`concurrencyLevel` 控制了同时有多少个节点的 `Mono<NodeResult>` 可以被激活（但这并不完全等同于同时运行的节点数，因为节点执行本身可能在不同的线程池上）。该并发度由 `dag.engine.concurrency-level` 配置。

### 日志记录与可视化

*   框架广泛使用 SLF4J 进行日志记录。调整日志级别（如 `DEBUG`, `TRACE`）可以获取更详细的执行信息。
*   `AbstractDagDefinition` 在初始化成功后，会以 INFO 级别打印 DAG 的结构信息（节点、依赖关系、被依赖关系）和计算出的拓扑执行顺序。
*   `AbstractDagDefinition` 还会以 INFO 级别输出 DAG 的 DOT 语言表示。您可以将此输出复制并使用 Graphviz 工具 (如 `dot` 命令行工具或在线查看器) 生成 DAG 的可视化图形，非常有助于理解和调试复杂的依赖关系。

---

## 贡献

欢迎各种形式的贡献，包括 Bug 报告、功能建议、代码 Pull Request 等。请遵循标准的 GitHub Fork & Pull Request 流程。

---

## 许可证

本项目采用 [Apache License 2.0](LICENSE) 授权。

