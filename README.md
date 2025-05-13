# Reactor-DAG 执行与监控框架

[![许可证](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/xyz.vvrf/reactor-dag.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:xyz.vvrf%20AND%20a:reactor-dag) <!-- 假设已发布到 Maven Central -->
<!-- 请根据实际情况添加更多徽章，例如：构建状态, 代码覆盖率, 等. -->
<!-- 构建状态: [![Build Status](https://travis-ci.org/your-username/your-repo.svg?branch=main)](https://travis-ci.org/your-username/your-repo) -->
<!-- 代码覆盖率: [![Coverage Status](https://coveralls.io/repos/github/your-username/your-repo/badge.svg?branch=main)](https://coveralls.io/github/your-username/your-repo?branch=main) -->

**Reactor-DAG** 是一个基于 [Project Reactor](https://projectreactor.io/) 构建的、功能强大的反应式有向无环图 (DAG) 执行框架。它允许开发者定义具有复杂依赖关系和条件逻辑的异步任务流，并以非阻塞、高效的方式执行它们。该框架特别适用于需要编排多个异步操作（如微服务调用、数据处理步骤、规则引擎执行等）的场景，并内置了 Spring Boot 自动配置支持以及一个可选的实时 Web 监控界面。

## 项目概览 (Overview)

在现代分布式和事件驱动的系统中，经常需要将一系列操作按照特定的依赖关系和条件组合起来执行。Reactor-DAG 框架旨在解决以下核心问题：

*   **复杂异步任务编排**: 如何以声明式、可维护的方式定义和执行由多个异步步骤组成的复杂流程？
*   **反应式与非阻塞**: 如何充分利用现代多核处理器，实现高吞吐量和低延迟的异步处理，避免线程阻塞？
*   **类型安全与灵活性**: 如何在保证类型安全的前提下，灵活地定义节点间的输入输出，以及执行条件？
*   **可观测性与调试**: 如何监控 DAG 的执行状态，快速定位问题，并理解其内部结构？

**Reactor-DAG 的独特价值和优势包括：**

*   **纯粹的反应式核心**: 完全基于 Project Reactor 构建，充分利用其强大的异步、背压和错误处理能力。
*   **声明式 DAG 定义**: 通过 `DagDefinitionBuilder` 以编程式、流畅的 API 构建 DAG 结构，清晰地描述节点和边。
*   **灵活的节点实现**: `DagNode` 接口允许开发者专注于业务逻辑，同时定义清晰的输入/输出槽 (typed slots)。
*   **强大的条件逻辑**: 支持多种边条件 (`DirectUpstreamCondition`, `LocalInputCondition`, `DeclaredDependencyCondition`)，允许根据上游结果或全局上下文动态决定边的激活。
*   **错误处理策略**: 提供 `FAIL_FAST` 和 `CONTINUE_ON_FAILURE` 策略，应对不同的业务容错需求。
*   **细粒度的配置**: 支持节点级别的超时和重试策略（通过 `StandardNodeExecutor` 的配置键或 `DagNode` 自身实现），以及框架级别的默认重试。
*   **Spring Boot 深度集成**:
    *   通过 `@DagNodeType` 注解和 `SpringScanningNodeRegistry` 实现节点自动发现和注册。
    *   `DagFrameworkAutoConfiguration` 提供核心组件（如 `DagEngineProvider`、默认调度器、默认重试策略）的自动配置。
    *   通过 `DagFrameworkProperties` 进行统一配置管理。
*   **可选的实时 Web 监控**:
    *   `DagMonitorWebAutoConfiguration` 提供开箱即用的 Web 监控端点。
    *   通过 `RealtimeDagMonitorListener` 收集 DAG 和节点执行事件。
    *   `DagMonitoringController` 将事件通过 Server-Sent Events (SSE) 推送给前端。
    *   提供了一个基于 Vue.js 和 Cytoscape.js 的前端页面 (`index.html`)，用于实时可视化 DAG 结构、节点状态和执行进度。
    *   支持通过 `/dag-monitor/dag-definition/{dagName}/cytoscapejs` 获取 DAG 的 Cytoscape.js JSON 描述。
*   **可扩展性**: 设计上考虑了扩展性，例如可以实现自定义的 `NodeRegistry`, `NodeExecutor`, `DagMonitorListener` 等。

**目标用户**:
该框架主要面向需要构建高性能、高可维护性异步流程的 Java 开发者，尤其是在使用 Spring Boot 和 Project Reactor 技术栈的场景下。

## 核心特性 (Features)

*   **反应式 DAG 执行**: 基于 `Mono` 和 `Flux` 实现完全非阻塞的执行流程。
*   **类型安全的节点定义**: 使用泛型 `DagNode<C, P>` 确保上下文和主要输出负载的类型安全。
*   **清晰的输入/输出槽 (Slots)**:
    *   `InputSlot<T>`: 定义节点期望的类型化输入。
    *   `OutputSlot<T>`: 定义节点产生的类型化输出。
    *   支持主输出槽和多个附加命名输出槽。
*   **灵活的边定义与条件逻辑**:
    *   `EdgeDefinition<C>`: 连接节点间的输出槽和输入槽。
    *   `ConditionBase<C>`: 接口，支持多种条件类型：
        *   `DirectUpstreamCondition`: 仅依赖直接上游结果和上下文。
        *   `LocalInputCondition`: 依赖上下文和目标下游节点的所有直接上游结果。
        *   `DeclaredDependencyCondition`: 依赖上下文、直接上游结果以及显式声明依赖的其他节点结果。
*   **声明式 DAG 构建**: 使用 `DagDefinitionBuilder<C>` 流畅地构建 `DagDefinition<C>`。
*   **节点注册与发现**:
    *   `NodeRegistry<C>`: 管理节点类型 ID 到节点实现的映射。
    *   `SimpleNodeRegistry<C>`: 基础的内存注册表。
    *   `SpringScanningNodeRegistry<C>`: 在 Spring 环境中自动扫描并注册使用 `@DagNodeType` 注解的 `DagNode` Bean。
*   **执行引擎与节点执行器**:
    *   `DagEngine<C>`: 负责整体 DAG 执行编排。
    *   `StandardDagEngine<C>`: `DagEngine` 的标准实现。
    *   `NodeExecutor<C>`: 负责单个节点的执行，包括超时、重试。
    *   `StandardNodeExecutor<C>`: `NodeExecutor` 的标准实现。
*   **可配置的错误处理**: 支持 `FAIL_FAST` (默认) 和 `CONTINUE_ON_FAILURE` 策略。
*   **节点级超时与重试**:
    *   `StandardNodeExecutor` 支持通过配置键 `__instanceTimeout` 和 `__instanceRetry` 为节点实例设置超时和重试。
    *   `DagNode` 实现可以提供默认的超时 (`getExecutionTimeout()`) 和重试 (`getRetrySpec()`)。
    *   框架提供可配置的默认重试策略。
*   **Spring Boot 自动配置**:
    *   `DagFrameworkAutoConfiguration`: 配置核心服务如 `DagEngineProvider`，以及默认的 `Scheduler` 和 `Retry` 策略。
    *   `DagFrameworkProperties`: 通过 `application.properties/yml` 进行配置。
*   **实时监控与可视化 (可选模块)**:
    *   `DagMonitorListener`: 监听 DAG 和节点执行事件。
    *   `RealtimeDagMonitorListener`: 基于 SSE 的实时监控事件推送服务。
    *   `DagMonitoringController`: 提供 SSE 端点和 DAG 定义 (Cytoscape.js JSON) 端点。
    *   `DagDefinitionCache`: 缓存 DAG 的 Cytoscape.js JSON 和 DOT 表示。
    *   `index.html`: 一个 Vue.js + Cytoscape.js 构建的实时监控前端页面。
*   **图工具**: `GraphUtils` 提供 DAG 验证、循环检测和拓扑排序功能。
*   **上下文传递**: `DagExecutionContext<C>` 封装单次 DAG 执行的运行时状态。
*   **输入访问器**: `InputAccessor<C>`, `ConditionInputAccessor<C>`, `LocalInputAccessor<C>` 为节点和条件逻辑提供对上游数据的安全访问。

## 背景 (Background/Motivation)

随着业务复杂度的增加和微服务架构的普及，单个请求往往需要调用多个下游服务或执行多个独立的计算步骤。这些步骤之间可能存在复杂的依赖关系和条件分支。传统的同步调用或手写异步回调链难以管理、容易出错且不易扩展和维护。

Reactor-DAG 框架旨在提供一个标准化的、基于反应式编程范式的解决方案，来应对这些挑战。通过将流程抽象为有向无环图，开发者可以更清晰地表达业务逻辑的流转，框架则负责高效、可靠地执行这些逻辑。

## 架构设计 (Architecture)

Reactor-DAG 框架的核心架构可以分为以下几个主要部分：

1.  **核心模型 (Core Model - `xyz.vvrf.reactor.dag.core`)**:
    *   `DagNode<C, P>`: 定义了 DAG 中一个计算单元的接口，包含输入/输出槽、执行逻辑、超时和重试配置。
    *   `InputSlot<?>` / `OutputSlot<?>`: 类型化的数据通道，定义了节点间数据交换的契约。
    *   `EdgeDefinition<C>`: 定义了节点之间的连接（依赖），包含源节点、目标节点、连接的槽以及激活条件。
    *   `ConditionBase<C>` (及其子接口): 定义了边是否激活的逻辑。
    *   `NodeResult<C, P>`: 封装了节点执行后的状态（成功、失败、跳过）、负载和错误。
    *   `DagDefinition<C>`: 不可变的 DAG 结构描述，包含所有节点定义、边定义、拓扑排序结果和错误处理策略。由 `DagDefinitionBuilder` 构建。
    *   `InputAccessor<C>` (及其变体): 为节点执行和条件评估提供对上游数据的访问。

2.  **注册与发现 (Registry - `xyz.vvrf.reactor.dag.registry`)**:
    *   `NodeRegistry<C>`: 接口，负责管理节点类型 ID 到 `DagNode` 实现（或其工厂）的映射，并提供节点元数据（如输入/输出槽信息）。
    *   `SimpleNodeRegistry<C>`: 一个简单的内存实现。
    *   `SpringScanningNodeRegistry<C>`: 在 Spring 环境中，自动扫描并注册带有 `@DagNodeType` 注解的 `DagNode` Bean。

3.  **执行引擎 (Execution - `xyz.vvrf.reactor.dag.execution`)**:
    *   `NodeExecutor<C>`: 接口，负责单个节点的实际执行，包括获取节点实例、应用超时/重试、调用 `DagNode.execute()` 并处理结果。
    *   `StandardNodeExecutor<C>`: `NodeExecutor` 的标准实现，它会：
        *   从 `NodeRegistry` 获取节点实例。
        *   确定有效的超时和重试策略（实例配置 > 节点默认 > 框架默认）。
        *   在指定的 `Scheduler` 上执行节点逻辑。
        *   处理 `TimeoutException` 和重试逻辑。
        *   将结果/错误包装成 `NodeResult`。
    *   `DagEngine<C>`: 接口，负责整个 DAG 的执行流程编排。
    *   `StandardDagEngine<C>`: `DagEngine` 的标准实现，它会：
        *   接收 `DagDefinition` 和初始上下文。
        *   创建一个 `DagExecutionContext` 来跟踪单次执行的状态。
        *   根据拓扑排序和边的条件，并发地调度 `NodeExecutor` 执行节点。
        *   处理 `ErrorHandlingStrategy`。
        *   通知 `DagMonitorListener` 相应的事件。
    *   `DagExecutionContext<C>`: 封装单次 DAG 执行的运行时状态，包括已完成的结果、是否触发 FailFast 等。

4.  **构建器与工具 (Builder & Utils - `xyz.vvrf.reactor.dag.builder`, `xyz.vvrf.reactor.dag.util`)**:
    *   `DagDefinitionBuilder<C>`: 用于以编程方式流式构建 `DagDefinition`。它会进行图结构验证、循环检测和拓扑排序。
    *   `GraphUtils`: 提供图相关的静态工具方法，如验证、循环检测、拓扑排序。

5.  **Spring Boot 集成 (Spring Boot Integration - `xyz.vvrf.reactor.dag.spring.boot`, `xyz.vvrf.reactor.dag.annotation`)**:
    *   `DagFrameworkProperties`: 类型安全的配置属性类。
    *   `DagFrameworkAutoConfiguration`: Spring Boot 自动配置类，负责：
        *   创建 `DagEngineProvider` Bean。
        *   创建默认的 `Scheduler` Bean (`dagNodeExecutionScheduler`) 用于节点执行。
        *   创建默认的框架级 `Retry` 策略 Bean (`dagFrameworkDefaultRetrySpec`)。
        *   收集所有 `DagMonitorListener` Bean 到一个列表 (`dagMonitorListeners`)。
    *   `DagEngineProvider`: 在 Spring 环境中，根据上下文类型提供相应的 `DagEngine` 实例。它会自动查找 `NodeRegistry`，并创建 `StandardNodeExecutor` 和 `StandardDagEngine`。
    *   `@DagNodeType`: 注解，用于标记 `DagNode` 实现类，使其能被 `SpringScanningNodeRegistry` 发现。

6.  **实时监控 (Monitoring - `xyz.vvrf.reactor.dag.monitor.*`)**:
    *   `DagMonitorListener`: 监听器接口，用于捕获 DAG 和节点执行过程中的各种事件（开始、成功、失败、跳过、超时等）。
    *   `RealtimeDagMonitorListener`: `DagMonitorListener` 的一个实现，它将监控事件：
        *   存储在内存中（`currentDagInstanceStates` 用于当前状态，`sinksByDagName` 用于事件流）。
        *   通过 `Sinks.Many` 将事件实时推送出去。
    *   `DagDefinitionCache`: 缓存 DAG 的图形化表示（如 Cytoscape.js JSON），供监控前端使用。
    *   `DagMonitorWebAutoConfiguration`: (在 `xyz.vvrf.reactor.dag.monitor.web.autoconfigure` 包下) Spring Boot 自动配置类，用于设置监控相关的 Web 组件，如 `DagMonitoringController`。
    *   `DagMonitoringController`: Spring WebFlux 控制器，提供：
        *   一个 SSE (Server-Sent Events) 端点 (`/dag-monitor/stream/{dagName}`)，用于流式传输指定 DAG 名称的实时监控事件 (`MonitoringEvent`)。
        *   一个获取 DAG 图形化定义的端点 (`/dag-monitor/dag-definition/{dagName}/cytoscapejs`)，返回 Cytoscape.js 兼容的 JSON 格式。
    *   `MonitoringEvent`: DTO，表示一个具体的监控事件。
    *   `index.html`: 一个基于 Vue.js 和 Cytoscape.js 的单页面应用，连接到 SSE 端点，实时展示 DAG 的拓扑结构、节点状态和执行进度。

**执行流程概要 (以 `StandardDagEngine` 为例):**

1.  用户调用 `DagEngine.execute(context, dagDefinition, requestId)`。
2.  `StandardDagEngine` 创建一个 `DagExecutionContext`。
3.  通知 `DagMonitorListener` `onDagStart` 事件。
4.  引擎遍历 `DagDefinition` 中的所有节点（通常按拓扑顺序或按需触发）。
5.  对于每个待执行节点 `N`：
    a.  引擎会为 `N` 创建或获取一个 `Mono<NodeResult>` (通过 `DagExecutionContext.getOrCreateNodeMono`)。
    b.  此 `Mono` 内部会：
    i.  等待 `N` 的所有直接上游依赖节点的 `Mono<NodeResult>` 完成。
    ii. 检查 `ErrorHandlingStrategy` (如 FAIL_FAST 是否已触发)。
    iii.评估连接到 `N` 的所有入边的条件 (`ConditionBase.evaluate()`)，确定哪些边是激活的。
    iv. 如果可以执行（至少一条入边激活，或无入边）：
    1.  创建一个 `InputAccessor` 包含来自激活边上游节点的成功结果。
    2.  调用 `NodeExecutor.executeNode(N, context, inputAccessor, ...)`。
    3.  `StandardNodeExecutor` 获取 `N` 的实现，应用超时和重试，执行 `N.execute()`。
    v.  如果不能执行，则将 `N` 标记为 SKIPPED。
    c.  节点执行的 `NodeResult` 被记录到 `DagExecutionContext`。
    d.  如果节点失败且策略是 `FAIL_FAST`，则触发全局 FailFast 标志。
6.  所有节点的 `Mono` 都被 `Flux.flatMap` 并发处理（受 `concurrencyLevel` 限制）。
7.  引擎合并所有成功节点的事件流。
8.  当所有节点处理完毕后，通知 `DagMonitorListener` `onDagComplete` 事件。
9.  返回合并后的事件 `Flux` 给调用者。

## 技术栈 (Tech Stack)

*   **核心语言**: Java 8+
*   **反应式框架**: [Project Reactor](https://projectreactor.io/) (Core `reactor-core`, `reactor-adapter`) - 版本: `3.7.5` (根据 pom.xml)
*   **Spring Framework (可选集成)**:
    *   `spring-context`: 用于依赖注入和组件管理 - 版本: `5.3.31`
    *   `spring-webflux`: 用于反应式 Web API (监控模块) - 版本: `5.3.31`
*   **Spring Boot (可选集成)**:
    *   `spring-boot-autoconfigure`: 提供自动配置能力 - 版本: `2.7.18`
    *   `spring-boot-starter-validation`: 用于配置属性校验 - 版本: `2.7.18`
*   **序列化/反序列化 (监控模块)**:
    *   `jackson-databind`: 用于将 `MonitoringEvent` 对象序列化为 JSON - 版本: `2.17.2`
*   **日志**: [SLF4J API](https://www.slf4j.org/) - 版本: `1.7.36` (用户需提供具体实现如 Logback, Log4j2)
*   **工具库**:
    *   [Lombok](https://projectlombok.org/): 简化 boilerplate 代码 (如 `@Slf4j`, `@Getter`, `@Builder`) - 版本: `1.18.30`
    *   `javax.annotation-api`: (如 `@PostConstruct`) - 版本: `1.3.2`
*   **监控 UI (可选)**:
    *   [Vue.js 3](https://vuejs.org/)
    *   [Cytoscape.js](https://js.cytoscape.org/): 用于图形可视化。
    *   [cytoscape-dagre](https://github.com/cytoscape/cytoscape.js-dagre): Dagre.js 布局算法的 Cytoscape.js 扩展。
    *   [Dagre.js](https://github.com/dagrejs/dagre): 用于有向图的布局。
*   **构建工具**: Maven (根据 pom.xml)
*   **测试 (开发时)**: JUnit 5, Reactor Test, Mockito

## 先决条件 (Prerequisites)

*   **Java Development Kit (JDK)**: 版本 8 或更高。
*   **Maven**: 版本 3.2+ (用于构建项目)。
*   **(可选) Spring Boot 环境**: 如果使用 Spring Boot 集成，需要一个 Spring Boot 2.x (推荐 2.7.x 以匹配依赖) 或更高版本的项目。
*   **(可选) 反应式 Web 服务器**: 如果使用 Web 监控功能，需要一个支持反应式流的 Web 服务器 (如 Netty，当使用 `spring-boot-starter-webflux` 时默认提供)。
*   **(可选) SLF4J 实现**: 如 Logback 或 Log4j2，用于运行时日志输出。

## 安装与部署 (Installation & Deployment)

### 1. 添加依赖

将 Reactor-DAG 库作为依赖添加到您的项目中。

**Maven:**

```xml
<dependency>
    <groupId>xyz.vvrf</groupId>
    <artifactId>reactor-dag</artifactId>
    <version>1.0.0</version> <!-- 请使用最新的稳定版本 -->
</dependency>

<!-- 如果您使用 Spring Boot 集成 -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter</artifactId>
    <!-- <version>...</version> 根据您的 Spring Boot 父项目版本 -->
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-validation</artifactId>
    <!-- <version>...</version> -->
</dependency>

<!-- 如果您使用 Web 监控功能 (需要 WebFlux) -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-webflux</artifactId>
    <!-- <version>...</version> -->
</dependency>

<!-- Lombok (如果您的项目也使用 Lombok) -->
<dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <!-- <version>...</version> -->
    <optional>true</optional>
</dependency>
```

**Gradle:**

```gradle
implementation 'xyz.vvrf:reactor-dag:1.0.0' // 请使用最新的稳定版本

// 如果您使用 Spring Boot 集成
implementation 'org.springframework.boot:spring-boot-starter'
implementation 'org.springframework.boot:spring-boot-starter-validation'

// 如果您使用 Web 监控功能 (需要 WebFlux)
implementation 'org.springframework.boot:spring-boot-starter-webflux'

// Lombok (如果您的项目也使用 Lombok)
compileOnly 'org.projectlombok:lombok'
annotationProcessor 'org.projectlombok:lombok'
```

### 2. 环境配置 (Spring Boot)

如果使用 Spring Boot，框架提供了自动配置。您可以在 `application.properties` 或 `application.yml` 中覆盖默认配置。

**`application.properties` 示例:**

```properties
# Reactor-DAG 框架配置
dag.node.default-timeout=PT30S                     # 节点默认超时时间 (ISO-8601 持续时间格式)
dag.engine.concurrency-level=4                   # DAG 引擎处理节点的并发级别

# 框架默认重试策略配置
dag.retry.enabled=true                           # 是否启用框架默认重试
dag.retry.max-attempts=3                         # 最大尝试次数 (1 表示不重试)
dag.retry.first-backoff=PT0.1S                   # 首次/固定退避延迟
dag.retry.use-exponential-backoff=false          # 是否使用指数退避
dag.retry.max-backoff=PT10S                      # (指数退避) 最大退避延迟
dag.retry.jitter-factor=0.5                      # (指数退避) 抖动因子 (0.0-1.0)

# 节点执行调度器配置
dag.scheduler.type=BOUNDED_ELASTIC               # 调度器类型 (BOUNDED_ELASTIC, PARALLEL, SINGLE, CUSTOM)
dag.scheduler.name-prefix=dag-node-exec          # 调度器名称前缀
dag.scheduler.bounded-elastic.thread-cap=200     # (BoundedElastic) 线程上限
dag.scheduler.bounded-elastic.queued-task-cap=100000 # (BoundedElastic) 队列任务上限
dag.scheduler.bounded-elastic.ttl-seconds=60     # (BoundedElastic) 线程 TTL (秒)
dag.scheduler.parallel.parallelism=8             # (Parallel) 并行度
# dag.scheduler.custom-bean-name=myCustomScheduler # (Custom) 自定义 Scheduler Bean 名称
```

**`application.yml` 示例:**

```yaml
dag:
  node:
    default-timeout: PT30S
  engine:
    concurrency-level: 4
  retry:
    enabled: true
    max-attempts: 3
    first-backoff: PT0.1S
    use-exponential-backoff: false
    max-backoff: PT10S
    jitter-factor: 0.5
  scheduler:
    type: BOUNDED_ELASTIC
    name-prefix: dag-node-exec
    bounded-elastic:
      thread-cap: 200
      queued-task-cap: 100000
      ttl-seconds: 60
    parallel:
      parallelism: 8
    # custom-bean-name: myCustomScheduler
```

### 3. 部署

将包含 Reactor-DAG 库的应用程序打包（例如，作为可执行 JAR），并像部署任何其他 Java/Spring Boot 应用程序一样进行部署。

*   **独立 Java 应用**: 确保所有依赖项都在类路径中。
*   **Spring Boot 应用**: 可以打包为可执行 JAR (`mvn spring-boot:repackage` 或 `gradle bootJar`)。

### 4. 安装后验证 (可选，针对监控功能)

如果启用了 Web 监控功能 (`xyz.vvrf.reactor.dag.monitor.web` 模块在类路径中且应用是响应式 Web 应用)：

1.  启动您的应用程序。
2.  在浏览器中打开 `index.html` 提供的监控页面。通常，如果您的应用运行在 `http://localhost:8080`，并且没有自定义上下文路径，您可以尝试访问 `http://localhost:8080/dag-monitor/index.html`（如果 `index.html` 是作为静态资源提供的）。
    *   **注意**: `index.html` 文件本身需要被您的Web服务器正确提供。在所提供的代码中，它似乎是一个独立的 HTML 文件，您可能需要手动将其部署到静态资源目录（如 Spring Boot 的 `src/main/resources/static`）或通过其他方式使其可访问。控制器 `DagMonitoringController` 的基础路径是 `/dag-monitor`。
3.  在监控页面的输入框中输入一个已定义的 DAG Name，然后点击“监控指定DAG”。
4.  当该 DAG 被执行时，您应该能在页面上看到实时的节点状态更新和 DAG 图。
5.  您也可以直接访问 SSE 流: `http://localhost:8080/dag-monitor/stream/{yourDagName}`
6.  访问 DAG 定义 (Cytoscape.js 格式): `http://localhost:8080/dag-monitor/dag-definition/{yourDagName}/cytoscapejs`

## 快速开始 (Quick Start - Spring Boot)

此快速入门将引导您在 Spring Boot 项目中创建一个简单的 DAG 并执行它。

**1. 定义上下文类 `MyContext.java`:**

```java
package com.example.dagdemo.model;

import lombok.Data;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class MyContext {
    private String inputMessage;
    private Map<String, Object> intermediateResults = new ConcurrentHashMap<>();
}
```

**2. 定义 DAG 节点:**

*   `StartNode.java`:
    ```java
    package com.example.dagdemo.nodes;

    import lombok.extern.slf4j.Slf4j;
    import org.springframework.stereotype.Component;
    import reactor.core.publisher.Mono;
    import xyz.vvrf.reactor.dag.annotation.DagNodeType;
    import xyz.vvrf.reactor.dag.core.*;
    import com.example.dagdemo.model.MyContext;
    import java.util.Collections;
    import java.util.Set;

    @Slf4j
    @DagNodeType(id = "startNodeType", contextType = MyContext.class) // 标记为 DAG 节点
    public class StartNode implements DagNode<MyContext, String> {

        public static final InputSlot<Void> TRIGGER_INPUT = InputSlot.optional("trigger", Void.class); // 示例可选输入
        public static final OutputSlot<String> GREETING_OUTPUT = OutputSlot.defaultOutput(String.class);

        @Override
        public Set<InputSlot<?>> getInputSlots() {
            return Collections.singleton(TRIGGER_INPUT);
        }

        @Override
        public OutputSlot<String> getOutputSlot() {
            return GREETING_OUTPUT;
        }

        @Override
        public Mono<NodeResult<MyContext, String>> execute(MyContext context, InputAccessor<MyContext> inputs) {
            log.info("StartNode executing for message: {}", context.getInputMessage());
            String greeting = "Hello, " + context.getInputMessage() + "!";
            context.getIntermediateResults().put("greeting", greeting);
            return Mono.just(NodeResult.success(greeting));
        }
    }
    ```

*   `EndNode.java`:
    ```java
    package com.example.dagdemo.nodes;

    import lombok.extern.slf4j.Slf4j;
    import org.springframework.stereotype.Component;
    import reactor.core.publisher.Mono;
    import xyz.vvrf.reactor.dag.annotation.DagNodeType;
    import xyz.vvrf.reactor.dag.core.*;
    import com.example.dagdemo.model.MyContext;
    import java.util.Collections;
    import java.util.Set;

    @Slf4j
    @DagNodeType(id = "endNodeType", contextType = MyContext.class)
    public class EndNode implements DagNode<MyContext, Boolean> {

        public static final InputSlot<String> MESSAGE_INPUT = InputSlot.required("messageIn", String.class);
        public static final OutputSlot<Boolean> FINAL_OUTPUT = OutputSlot.defaultOutput(Boolean.class);

        @Override
        public Set<InputSlot<?>> getInputSlots() {
            return Collections.singleton(MESSAGE_INPUT);
        }

        @Override
        public OutputSlot<Boolean> getOutputSlot() {
            return FINAL_OUTPUT;
        }

        @Override
        public Mono<NodeResult<MyContext, Boolean>> execute(MyContext context, InputAccessor<MyContext> inputs) {
            return inputs.getPayload(MESSAGE_INPUT)
                .map(message -> {
                    log.info("EndNode received: {}", message);
                    boolean success = message.contains("Hello");
                    context.getIntermediateResults().put("finalResult", success);
                    log.info("EndNode final processing result: {}", success);
                    return NodeResult.success(success);
                })
                .orElseGet(() -> {
                    log.error("EndNode did not receive message input!");
                    return NodeResult.failure(new IllegalStateException("Missing message input for EndNode"));
                });
        }
    }
    ```

**3. 配置 `NodeRegistry` Bean (`NodeRegistryConfig.java`):**

```java
package com.example.dagdemo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;
import xyz.vvrf.reactor.dag.registry.SpringScanningNodeRegistry;
import com.example.dagdemo.model.MyContext;

@Configuration
public class NodeRegistryConfig {

    @Bean
    public NodeRegistry<MyContext> myContextNodeRegistry() {
        // SpringScanningNodeRegistry 会自动扫描 @DagNodeType(contextType = MyContext.class) 的 Bean
        return new SpringScanningNodeRegistry<>(MyContext.class);
    }
}
```

**4. 定义 DAG (`MySimpleDag.java`):**

```java
package com.example.dagdemo.dags;

import org.springframework.stereotype.Component;
import xyz.vvrf.reactor.dag.builder.DagDefinitionBuilder;
import xyz.vvrf.reactor.dag.core.DagDefinition;
import xyz.vvrf.reactor.dag.core.OutputSlot;
import xyz.vvrf.reactor.dag.registry.NodeRegistry;
import com.example.dagdemo.model.MyContext;
import com.example.dagdemo.nodes.StartNode;
import com.example.dagdemo.nodes.EndNode;
import xyz.vvrf.reactor.dag.monitor.web.service.DagDefinitionCache; // 用于缓存图形定义

import javax.annotation.PostConstruct; // 导入

@Component
public class MySimpleDag {

    private final NodeRegistry<MyContext> nodeRegistry;
    private DagDefinition<MyContext> definition;
    private final DagDefinitionCache dagDefinitionCache; // 注入缓存服务

    public MySimpleDag(NodeRegistry<MyContext> myContextNodeRegistry, DagDefinitionCache dagDefinitionCache) {
        this.nodeRegistry = myContextNodeRegistry;
        this.dagDefinitionCache = dagDefinitionCache;
    }

    @PostConstruct // 在 Bean 初始化后构建 DAG 定义
    public void init() {
        DagDefinitionBuilder<MyContext> builder = new DagDefinitionBuilder<>(
                MyContext.class,
                "MySimpleGreetingDag", // DAG 名称
                nodeRegistry
        );

        // 添加节点实例 (instanceName, nodeTypeId)
        builder.addNode("startInstance", "startNodeType");
        builder.addNode("endInstance", "endNodeType");

        // 添加边 (upstreamInstance, upstreamOutputSlotId, downstreamInstance, downstreamInputSlotId)
        // StartNode 的默认输出连接到 EndNode 的 messageIn 输入
        builder.addEdge("startInstance", StartNode.GREETING_OUTPUT.getId(),
                        "endInstance", EndNode.MESSAGE_INPUT.getId());

        this.definition = builder.build();

        // 构建完成后，缓存其 Cytoscape.js JSON 和 DOT 表示
        if (this.dagDefinitionCache != null) {
            this.dagDefinitionCache.cacheDag(this.definition, builder.getLastGeneratedDotCode(), builder.getLastGeneratedCytoscapeJsJson());
        }
    }

    public DagDefinition<MyContext> getDefinition() {
        return definition;
    }
}
```
*确保主应用类上有 `@SpringBootApplication` 以启用组件扫描。*

**5. 创建一个服务来执行 DAG (`DagDemoService.java`):**

```java
package com.example.dagdemo.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.execution.DagEngine;
import xyz.vvrf.reactor.dag.spring.boot.DagEngineProvider;
import com.example.dagdemo.model.MyContext;
import com.example.dagdemo.dags.MySimpleDag;

import java.util.UUID;

@Service
@Slf4j
public class DagDemoService {

    private final DagEngine<MyContext> myContextDagEngine;
    private final MySimpleDag mySimpleDag;

    @Autowired
    public DagDemoService(DagEngineProvider dagEngineProvider, MySimpleDag mySimpleDag) {
        this.myContextDagEngine = dagEngineProvider.getEngine(MyContext.class);
        this.mySimpleDag = mySimpleDag;
    }

    public Flux<Event<?>> processMessage(String message) {
        MyContext context = new MyContext();
        context.setInputMessage(message);

        String requestId = "demo-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("Executing MySimpleGreetingDag with requestId: {}, message: {}", requestId, message);

        // DagEngine 返回的是 Flux<Event<?>>
        return myContextDagEngine.execute(context, mySimpleDag.getDefinition(), requestId)
                .doOnNext(event -> log.info("[{}] DAG Event: {}", requestId, event))
                .doOnError(error -> log.error("[{}] DAG Execution Error: ", requestId, error))
                .doOnComplete(() -> {
                    log.info("[{}] DAG Execution Completed. Final context results: {}", requestId, context.getIntermediateResults());
                });
    }
}
```

**6. (可选) 创建一个 Controller 触发 DAG 执行并查看监控:**

```java
package com.example.dagdemo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import xyz.vvrf.reactor.dag.core.Event;
import com.example.dagdemo.service.DagDemoService;
import xyz.vvrf.reactor.dag.spring.EventAdapter; // 引入 EventAdapter

@RestController
public class DemoController {

    @Autowired
    private DagDemoService dagDemoService;

    @GetMapping(value = "/run-dag", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Event<?>>> runDag(@RequestParam String message) {
        Flux<Event<?>> eventFlux = dagDemoService.processMessage(message);
        // 使用 EventAdapter 将 Flux<Event<?>> 转换为 Flux<ServerSentEvent<Event<?>>>
        // 注意：这里的 ServerSentEvent 的 data 部分将是 Event<?> 对象本身
        // 如果监控前端期望的是 MonitoringEvent 的 JSON 字符串，则 EventAdapter 的使用方式或
        // DagDemoService 返回的事件类型需要调整。
        // 对于本框架的监控 UI (index.html)，它期望的是 MonitoringEvent 的 JSON 序列化。
        // DagMonitoringController 内部会处理这个序列化。
        // 如果你希望此端点也兼容该UI，你可能需要模拟 MonitoringEvent 或调整。
        // 此处，我们仅展示原始 Event<?> 的 SSE 转换。
        return EventAdapter.toServerSentEvents(eventFlux)
                .map(sse -> (ServerSentEvent<Event<?>>) sse); // 确保泛型正确
    }
}
```

**7. 运行并测试:**
启动 Spring Boot 应用。访问 `http://localhost:8080/run-dag?message=ReactorDAG`。
   如果监控模块已配置且 `index.html` 可访问，打开监控页面，输入 "MySimpleGreetingDag" 进行监控。

## 详细用法 (Usage)

### 核心概念回顾

*   **`DagNode<C, P>`**: DAG 中的计算单元。
    *   `C`: 上下文类型。
    *   `P`: 主要输出 Payload 类型。
*   **`InputSlot<T>` / `OutputSlot<T>`**: 定义节点的类型化输入和输出。
*   **`EdgeDefinition<C>`**: 定义节点间的连接和条件。
*   **`ConditionBase<C>`**: 边的激活条件。
*   **`NodeResult<C, P>`**: 封装节点执行结果 (状态, Payload, 错误, 事件)。
*   **`DagDefinition<C>`**: 不可变的 DAG 结构。
*   **`NodeRegistry<C>`**: 管理 `DagNode` 实现。
*   **`DagEngine<C>`**: 执行 DAG。
*   **`NodeExecutor<C>`**: 执行单个节点。

### 1. 定义上下文 (Context)

上下文是普通的 Java 对象，用于在 DAG 的不同节点间共享数据和状态。

```java
// OrderProcessingContext.java
import lombok.Data;
import java.math.BigDecimal;
import java.util.List;

@Data
public class OrderProcessingContext {
    private String orderId;
    private String customerId;
    private List<String> itemIds;
    private BigDecimal totalAmount;
    private String paymentStatus;
    private String shippingStatus;
    // ... 其他字段和中间结果
}
```

### 2. 定义 DAG 节点 (`DagNode`)

每个 `DagNode` 实现代表一个异步任务。

**关键点:**

*   **实现 `DagNode<C, P>` 接口**:
    *   `C` 是上下文类型。
    *   `P` 是此节点主输出槽 (`getOutputSlot()`) 的 Payload 类型。
*   **`getInputSlots()`**: 返回一个 `Set<InputSlot<?>>`，声明节点需要的所有输入。每个 `InputSlot` 有一个唯一的 ID 和期望的类型。使用 `InputSlot.required(...)` 或 `InputSlot.optional(...)`。
*   **`getOutputSlot()`**: 返回一个 `OutputSlot<P>`，声明节点的主输出。使用 `OutputSlot.defaultOutput(...)`。
*   **`getAdditionalOutputSlots()` (可选)**: 如果节点有多个命名输出，在此返回 `Set<OutputSlot<?>>`。使用 `OutputSlot.create(id, type)`。
*   **`execute(C context, InputAccessor<C> inputs)`**: 实现核心业务逻辑。
    *   使用 `inputs.getPayload(InputSlot)` 安全地获取输入数据。
    *   返回 `Mono<NodeResult<C, P>>`。
    *   使用 `NodeResult.success(payload)`、`NodeResult.failure(error)`。
*   **`getRetrySpec()` (可选)**: 返回 `reactor.util.retry.Retry` 实例以定义节点级重试。
*   **`getExecutionTimeout()` (可选)**: 返回 `java.time.Duration` 以定义节点级超时。
*   **注解 (Spring 环境)**: 使用 `@DagNodeType(id = "uniqueTypeId", contextType = YourContext.class)` 标记类，以便 `SpringScanningNodeRegistry` 发现。`id` 必须在同一 `NodeRegistry` 中唯一。

**示例：`ValidateOrderNode.java`**

```java
import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.annotation.DagNodeType;
import xyz.vvrf.reactor.dag.core.*;
import reactor.core.publisher.Mono;
import java.util.Collections;
import java.util.Set;
import java.time.Duration;
import reactor.util.retry.Retry;

// 假设 OrderProcessingContext 已定义
// import com.example.OrderProcessingContext;

@Slf4j
@DagNodeType(id = "validateOrder", contextType = OrderProcessingContext.class)
public class ValidateOrderNode implements DagNode<OrderProcessingContext, Boolean> {

    // 输入槽：期望一个 OrderProcessingContext 作为整体输入，但通常节点不直接声明上下文为输入槽，
    // 而是声明从其他节点获取的具体数据片段。这里假设它是一个起始节点或从外部获取订单ID。
    public static final InputSlot<String> ORDER_ID_INPUT = InputSlot.required("orderIdIn", String.class);
    // 输出槽：验证结果
    public static final OutputSlot<Boolean> VALIDATION_RESULT_OUTPUT = OutputSlot.defaultOutput(Boolean.class);

    @Override
    public Set<InputSlot<?>> getInputSlots() {
        // 如果是起点，可能没有显式输入槽，或有一个触发器类型的可选输入槽
        // 此处假设它需要一个订单ID作为输入来开始验证
        return Collections.singleton(ORDER_ID_INPUT);
    }

    @Override
    public OutputSlot<Boolean> getOutputSlot() {
        return VALIDATION_RESULT_OUTPUT;
    }

    @Override
    public Duration getExecutionTimeout() {
        return Duration.ofSeconds(5); // 节点特定超时
    }

    @Override
    public Retry getRetrySpec() {
        // 重试2次 (总共3次尝试)，固定延迟100ms
        return Retry.fixedDelay(2, Duration.ofMillis(100))
                .filter(throwable -> throwable instanceof transient_exception_marker_interface); // 只重试特定类型的瞬时错误
    }

    @Override
    public Mono<NodeResult<OrderProcessingContext, Boolean>> execute(OrderProcessingContext context, InputAccessor<OrderProcessingContext> inputs) {
        return inputs.getPayload(ORDER_ID_INPUT)
            .map(orderId -> {
                log.info("Validating order: {}", orderId);
                // 模拟验证逻辑
                boolean isValid = orderId != null && !orderId.isEmpty() && context.getItemIds() != null && !context.getItemIds().isEmpty();
                if (isValid) {
                    log.info("Order {} is valid.", orderId);
                    return NodeResult.success(true);
                } else {
                    log.warn("Order {} is invalid. Context: {}", orderId, context);
                    return NodeResult.failure(new IllegalArgumentException("Order validation failed for " + orderId));
                }
            })
            .orElseGet(() -> {
                log.error("OrderID input not available for validation.");
                return NodeResult.failure(new IllegalStateException("Missing orderIdInput for ValidateOrderNode"));
            });
    }
}
```

### 3. 定义 DAG 结构 (`DagDefinitionBuilder`)

使用 `DagDefinitionBuilder<C>` 以编程式方式构建 `DagDefinition<C>`。

**步骤:**

1.  **创建 `DagDefinitionBuilder` 实例**:
    ```java
    NodeRegistry<OrderProcessingContext> registry = ...; // 获取或创建 NodeRegistry
    DagDefinitionBuilder<OrderProcessingContext> builder = new DagDefinitionBuilder<>(
        OrderProcessingContext.class,
        "OrderProcessingDAG_V1", // DAG 名称
        registry
    );
    ```
2.  **配置 DAG 级别属性 (可选)**:
    ```java
    builder.errorStrategy(ErrorHandlingStrategy.CONTINUE_ON_FAILURE);
    ```
3.  **添加节点实例**:
    *   `builder.addNode(String instanceName, String nodeTypeId)`: `instanceName` 是此节点在 DAG 中的唯一实例名，`nodeTypeId` 是在 `NodeRegistry` 中注册的 ID (通常来自 `@DagNodeType.id`)。
    ```java
    builder.addNode("validationStep", "validateOrder"); // instanceName, nodeTypeId
    builder.addNode("paymentStep", "processPayment");
    builder.addNode("inventoryStep", "updateInventory");
    builder.addNode("notificationStep", "sendNotification");
    ```
4.  **配置节点实例特定属性 (可选)**:
    *   `builder.withTimeout(String instanceName, Duration timeout)`
    *   `builder.withRetry(String instanceName, Retry retrySpec)`
    *   `builder.withConfiguration(String instanceName, String key, Object value)`: 用于通用配置，但超时和重试有专用方法。
    ```java
    builder.withTimeout("paymentStep", Duration.ofSeconds(10));
    Retry customRetry = Retry.backoff(3, Duration.ofMillis(200)).maxBackoff(Duration.ofSeconds(2));
    builder.withRetry("inventoryStep", customRetry);
    ```
5.  **添加边和条件**:
    *   `builder.addEdge(upInstance, upSlotId, downInstance, downSlotId, condition)`
    *   `builder.addEdge(upInstance, upSlotId, downInstance, downSlotId)` (无条件，默认为 `DirectUpstreamCondition.alwaysTrue()`)
    *   `builder.addEdge(upInstance, downInstance, downSlotId, condition)` (使用上游默认输出槽)
    *   `builder.addEdge(upInstance, downInstance, downSlotId)` (上游默认输出，无条件)

    **条件类型 (`ConditionBase<C>`)**:
    *   **`DirectUpstreamCondition<C, U>`**: 边是否激活仅取决于**直接上游节点**的结果和当前上下文。
        ```java
        // 示例：只有当 ValidateOrderNode 成功 (返回 true) 时，才激活到 ProcessPaymentNode 的边
        DirectUpstreamCondition<OrderProcessingContext, Boolean> ifOrderValid =
            (ctx, upstreamResult) -> upstreamResult.isSuccess() && upstreamResult.getPayload().orElse(false);

        builder.addEdge("validationStep", ValidateOrderNode.VALIDATION_RESULT_OUTPUT.getId(),
                        "paymentStep", ProcessPaymentNode.TRIGGER_INPUT.getId(), // 假设 ProcessPaymentNode 有个触发输入
                        ifOrderValid);
        ```
    *   **`LocalInputCondition<C>`**: 边是否激活取决于当前上下文和目标下游节点的**所有直接上游节点**的结果。
        ```java
        // 示例：只有当支付和库存都成功时，才激活到 SendNotificationNode 的边
        LocalInputCondition<OrderProcessingContext> ifPaymentAndInventoryOk =
            (ctx, localInputs) -> localInputs.isUpstreamSuccess("paymentStep") &&
                                   localInputs.isUpstreamSuccess("inventoryStep");

        // 假设 paymentStep 和 inventoryStep 都是 notificationStep 的直接上游
        // 这条边是从 paymentStep 到 notificationStep
        builder.addEdge("paymentStep", /* output slot */, "notificationStep", /* input slot */, ifPaymentAndInventoryOk);
        // 另一条边从 inventoryStep 到 notificationStep 可能也有类似的或不同的条件
        ```
    *   **`DeclaredDependencyCondition<C>`**: 边是否激活取决于上下文、直接上游结果以及**通过 `getRequiredNodeDependencies()` 声明依赖的其他任意节点**的结果。
        ```java
        // 示例：除了直接上游 (e.g. "inventoryStep")，还需要检查 "validationStep" 是否成功
        DeclaredDependencyCondition<OrderProcessingContext> complexCondition = new DeclaredDependencyCondition<>() {
            @Override
            public Set<String> getRequiredNodeDependencies() {
                return Collections.singleton("validationStep"); // 声明需要访问 "validationStep" 的结果
            }

            @Override
            public boolean evaluate(OrderProcessingContext context, ConditionInputAccessor<OrderProcessingContext> inputs) {
                boolean directUpstreamOk = inputs.getNodeStatus("inventoryStep") // "inventoryStep" 是这条边的直接上游
                                               .map(NodeResult.NodeStatus.SUCCESS::equals)
                                               .orElse(false);
                boolean validationOk = inputs.getNodeStatus("validationStep")
                                             .map(NodeResult.NodeStatus.SUCCESS::equals)
                                             .orElse(false);
                return directUpstreamOk && validationOk;
            }

             @Override
             public String getConditionTypeDisplayName() { return "CustomInventoryAndValidationCheck"; }
        };
        builder.addEdge("inventoryStep", /* output slot */, "notificationStep", /* input slot */, complexCondition);
        ```

6.  **构建 `DagDefinition`**:
    *   `DagDefinition<C> definition = builder.build();`
    *   此方法会执行图验证（结构、类型兼容性）、循环检测和拓扑排序。
    *   构建成功后，可以通过 `builder.getLastGeneratedDotCode()` 和 `builder.getLastGeneratedCytoscapeJsJson()` 获取图形化表示。
7.  **(监控模块) 缓存 DAG 定义**:
    如果使用了监控模块，应在 `DagDefinition` 构建完成后将其图形化表示缓存起来。
    ```java
    // DagDefinitionCache dagDefinitionCache = ...; // 获取 DagDefinitionCache Bean
    // dagDefinitionCache.cacheDag(definition, builder.getLastGeneratedDotCode(), builder.getLastGeneratedCytoscapeJsJson());
    ```

### 4. 执行 DAG

*   **获取 `DagEngine<C>`**:
    *   在 Spring Boot 环境中，注入 `DagEngineProvider`，然后调用 `provider.getEngine(YourContext.class)`。
    *   `DagEngineProvider` 会根据 `YourContext.class` 查找匹配的 `NodeRegistry<YourContext>` Bean，并创建和缓存 `DagEngine<YourContext>` 实例。
*   **调用 `execute` 方法**:
    *   `Flux<Event<?>> events = engine.execute(initialContext, dagDefinition, requestId);`
    *   `initialContext`: 初始的上下文对象。
    *   `dagDefinition`: 已构建和初始化的 `DagDefinition`。
    *   `requestId`: 用于日志和监控的唯一请求 ID (可选，不提供则自动生成)。
*   **处理返回的 `Flux<Event<?>>`**:
    *   订阅事件流以接收 DAG 执行过程中产生的事件。
    *   处理 `onError` 和 `onComplete` 信号。

```java
// 在 Spring Service 中
@Service
public class OrderService {
    private final DagEngine<OrderProcessingContext> orderDagEngine;
    private final DagDefinition<OrderProcessingContext> orderProcessingDag; // 注入已构建的 DAG 定义 Bean

    public OrderService(DagEngineProvider engineProvider, /* @Qualifier specific DAG bean */ DagDefinition<OrderProcessingContext> orderProcessingDag) {
        this.orderDagEngine = engineProvider.getEngine(OrderProcessingContext.class);
        this.orderProcessingDag = orderProcessingDag;
    }

    public Mono<Void> processOrder(String orderId, List<String> items) {
        OrderProcessingContext context = new OrderProcessingContext();
        context.setOrderId(orderId);
        context.setItemIds(items);

        String reqId = "order-" + UUID.randomUUID().toString().substring(0,8);

        return orderDagEngine.execute(context, orderProcessingDag, reqId)
            .doOnNext(event -> log.info("[{}] Order Event: {}", reqId, event.getEventType() + ":" + event.getData()))
            .doOnError(err -> log.error("[{}] Order DAG failed: ", reqId, err))
            .doOnComplete(() -> log.info("[{}] Order DAG completed. Final payment status: {}, shipping: {}",
                    reqId, context.getPaymentStatus(), context.getShippingStatus()))
            .then(); // 转换 Flux 为 Mono<Void> 表示完成
    }
}
```

### 5. Spring Boot 集成细节

*   **`@DagNodeType` 注解**:
    *   `id`: 节点类型ID，在 `NodeRegistry` 中唯一。
    *   `value`: `id` 的别名。
    *   `contextType`: 此节点关联的上下文类。`SpringScanningNodeRegistry` 用此来筛选节点。
    *   `scope`: Spring Bean 的作用域 (默认 `SCOPE_SINGLETON`)。如果是原型作用域 (`SCOPE_PROTOTYPE`)，注册表每次都会从 Spring 上下文获取新实例。
*   **`NodeRegistry<C>` Bean**: 必须为每个上下文类型 `C` 提供一个 `NodeRegistry<C>` Bean。通常使用 `SpringScanningNodeRegistry<C>`。
    ```java
    @Configuration
    public class MyRegistries {
        @Bean
        public NodeRegistry<OrderProcessingContext> orderNodeRegistry() {
            return new SpringScanningNodeRegistry<>(OrderProcessingContext.class);
        }
        @Bean
        public NodeRegistry<UserOnboardingContext> userNodeRegistry() {
            return new SpringScanningNodeRegistry<>(UserOnboardingContext.class);
        }
    }
    ```
*   **`DagDefinition<C>` Bean**: 将您的 `DagDefinition` 构建逻辑封装在一个 Spring Bean 中（通常使用 `@PostConstruct` 来完成 `builder.build()` 和缓存）。
*   **`DagEngineProvider`**: 自动配置提供此 Bean。通过它获取特定上下文的 `DagEngine`。
*   **`DagFrameworkAutoConfiguration`**: 自动配置。
    *   提供 `DagEngineProvider`。
    *   提供名为 `dagNodeExecutionScheduler` 的 `Scheduler` Bean (可配置类型)。
    *   提供名为 `dagFrameworkDefaultRetrySpec` 的 `Retry` Bean (可配置)。
    *   收集所有 `DagMonitorListener` Bean 到名为 `dagMonitorListeners` 的 `List` Bean。
*   **`DagMonitorWebAutoConfiguration`**: (如果监控模块在类路径中)
    *   提供 `DagDefinitionCache` Bean。
    *   提供 `DagMonitoringController` Bean (依赖 `RealtimeDagMonitorListener`, `DagDefinitionCache`, `ObjectMapper`)。
    *   `RealtimeDagMonitorListener` 通常也会被 `@ComponentScan` 扫描到并作为 `DagMonitorListener` 注入。

### 6. 监控 Web UI (`index.html`)

提供的 `index.html` 是一个基于 Vue.js 3 和 Cytoscape.js 的单页面应用，用于实时监控 DAG 的执行。

**如何使用:**

1.  **确保监控后端已启用**:
    *   项目中包含 `xyz.vvrf.reactor.dag.monitor.web` 相关依赖。
    *   Spring Boot 应用是 WebFlux 应用 (`spring-boot-starter-webflux` 在类路径中)。
    *   `DagMonitorWebAutoConfiguration` 会自动配置必要的 Bean。
2.  **部署 `index.html`**:
    *   将 `index.html` 文件放置到 Spring Boot 项目的静态资源目录，例如 `src/main/resources/static/dag-monitor/index.html`。
    *   这样，如果应用运行在 `http://localhost:8080`，监控页面可以通过 `http://localhost:8080/dag-monitor/index.html` 访问。
3.  **访问和使用监控页面**:
    *   在浏览器中打开该 URL。
    *   在 "DAG Name" 输入框中输入您希望监控的 DAG 的名称 (与 `DagDefinitionBuilder` 中设置或 `DagDefinition.getDagName()` 返回的一致)。
    *   点击 "监控指定DAG"按钮。
    *   页面将连接到后端的 SSE 端点 (`/dag-monitor/stream/{dagName}`)。
    *   当匹配名称的 DAG 开始执行时：
        *   左侧 "实例列表" 会显示正在运行或已完成的 DAG 实例 (按 Request ID)。
        *   点击一个实例，右侧会显示该实例的 Cytoscape.js 图形。
        *   节点颜色会根据其状态 (PENDING, RUNNING, SUCCESS, FAILURE, SKIPPED) 实时更新。
        *   点击图中的节点，下方会显示该节点的详细信息 (类型ID, 状态, 耗时, Payload摘要, 错误摘要)。
        *   可以点击 "停止监控" 来断开连接并清空视图。
4.  **获取 DAG 定义**:
    监控页面在连接时，会尝试从 `/dag-monitor/dag-definition/{dagName}/cytoscapejs` 获取 DAG 的静态结构（节点和边）用于绘图。这要求在 DAG 定义构建完成后，其 Cytoscape.js JSON 表示已通过 `DagDefinitionCache.cacheDag(...)` 方法缓存。

**`index.html` 主要功能点:**

*   **Vue.js 3**: 用于构建响应式用户界面。
*   **Cytoscape.js**: 用于渲染和交互式操作 DAG 图。
    *   使用 `cytoscape-dagre` 扩展进行自动布局。
*   **SSE (Server-Sent Events)**: 从后端接收实时监控事件 (`MonitoringEvent`)。
*   **状态管理**:
    *   `activeInstances`: 存储所有监控到的 DAG 实例及其节点状态。
    *   `selectedRequestId`: 当前选中的 DAG 实例。
    *   `dagCytoscapeJsData`: 存储当前监控 DAG 的静态图结构 (从后端获取)。
*   **事件处理**:
    *   `DAG_START`: 初始化实例，并根据 `dagCytoscapeJsData` 为所有节点设置初始 PENDING 状态。
    *   `NODE_UPDATE`: 更新对应节点在图中的状态和数据。
    *   `DAG_COMPLETE`: 更新 DAG 实例的最终状态和耗时。
*   **动态图更新**: 节点状态变化时，Cytoscape 图中对应节点的样式（颜色）会改变。
*   **节点详情展示**: 点击图上节点可查看详细信息。

## 配置 (Configuration)

通过 `DagFrameworkProperties` 类进行配置，绑定前缀为 `dag`。

### 1. 节点配置 (`dag.node.*`)

| 属性                      | 描述                                     | 类型                 | 默认值  |
| :------------------------ | :--------------------------------------- | :------------------- | :------ |
| `dag.node.default-timeout` | 节点默认执行超时时间 (ISO-8601 格式)。 | `java.time.Duration` | `PT30S` |

### 2. 引擎配置 (`dag.engine.*`)

| 属性                         | 描述                         | 类型  | 默认值                                   |
| :--------------------------- | :--------------------------- | :---- | :--------------------------------------- |
| `dag.engine.concurrency-level` | DAG 引擎处理节点流的并发级别。 | `int` | `Math.max(1, Runtime.getRuntime().availableProcessors())` |

### 3. 调度器配置 (`dag.scheduler.*`)

用于配置 `dagNodeExecutionScheduler` Bean。

| 属性                                  | 描述                                                         | 类型                                        | 默认值                    |
| :------------------------------------ | :----------------------------------------------------------- | :------------------------------------------ | :------------------------ |
| `dag.scheduler.type`                  | 调度器类型。可选值: `BOUNDED_ELASTIC`, `PARALLEL`, `SINGLE`, `CUSTOM`。 | `DagFrameworkProperties.SchedulerType`      | `BOUNDED_ELASTIC`         |
| `dag.scheduler.name-prefix`           | 创建的调度器线程名称前缀。                                   | `String`                                    | `dag-exec`                |
| `dag.scheduler.custom-bean-name`      | 当 `type` 为 `CUSTOM` 时，自定义 `Scheduler` Bean 的名称。         | `String`                                    | `null`                    |
| **BoundedElastic 配置 (`dag.scheduler.bounded-elastic.*`)** |                                                              |                                             |                           |
| `...thread-cap`                       | 最大线程数。                                                 | `int`                                       | `Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE` (通常是 CPU核心数 * 10) |
| `...queued-task-cap`                  | 每个线程的队列任务上限。                                       | `int`                                       | `Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE` (通常是 100000) |
| `...ttl-seconds`                      | 空闲线程的存活时间 (秒)。                                      | `int`                                       | `60`                      |
| **Parallel 配置 (`dag.scheduler.parallel.*`)**        |                                                              |                                             |                           |
| `...parallelism`                      | 并行线程数。                                                 | `int`                                       | `Runtime.getRuntime().availableProcessors()` |
| **Single 配置 (`dag.scheduler.single.*`)**            | (当前无特定属性)                                             |                                             |                           |

### 4. 框架默认重试配置 (`dag.retry.*`)

用于配置 `dagFrameworkDefaultRetrySpec` Bean。

| 属性                               | 描述                                                       | 类型                 | 默认值      |
| :--------------------------------- | :--------------------------------------------------------- | :------------------- | :---------- |
| `dag.retry.enabled`                | 是否启用框架级别的默认重试。                                   | `boolean`            | `true`      |
| `dag.retry.max-attempts`           | 最大总尝试次数 (1 表示不重试)。                                | `long`               | `3`         |
| `dag.retry.first-backoff`          | 首次/固定退避延迟时间 (ISO-8601 格式)。                      | `java.time.Duration` | `PT0.1S`    |
| `dag.retry.use-exponential-backoff`| 是否使用指数退避。如果为 `false`，则使用 `firstBackoff` 作为固定延迟。 | `boolean`            | `false`     |
| `dag.retry.max-backoff`            | (仅用于指数退避) 最大退避延迟时间 (ISO-8601 格式)。           | `java.time.Duration` | `PT10S`     |
| `dag.retry.jitter-factor`          | (仅用于指数退避) 抖动因子 (0.0 到 1.0)。建议 0.0 到 0.8。      | `Double`             | `0.5`       |

**示例 `application.yml`:**
```yaml
dag:
  node:
    default-timeout: PT15S
  engine:
    concurrency-level: 8
  scheduler:
    type: PARALLEL
    name-prefix: my-dag-scheduler
    parallel:
      parallelism: 16
  retry:
    enabled: true
    max-attempts: 5
    use-exponential-backoff: true
    first-backoff: PT0.2S
    max-backoff: PT5S
    jitter-factor: 0.2
```

## 项目结构 (Project Structure)

项目主要包结构如下：

*   `xyz.vvrf.reactor.dag.annotation`: 包含用于节点自动发现的注解 (如 `@DagNodeType`)。
*   `xyz.vvrf.reactor.dag.builder`: 包含 `DagDefinitionBuilder`，用于编程式构建 DAG。
*   `xyz.vvrf.reactor.dag.core`: 包含 DAG 的核心模型接口和类，如 `DagNode`, `DagDefinition`, `NodeResult`, `InputSlot`, `OutputSlot`, `EdgeDefinition`, `ConditionBase` 及其子接口, `InputAccessor` 及其变体, `Event`, `ErrorHandlingStrategy`。
*   `xyz.vvrf.reactor.dag.execution`: 包含 DAG 执行相关的核心组件，如 `DagEngine`, `NodeExecutor`, `StandardDagEngine`, `StandardNodeExecutor`, `DagExecutionContext`, `DefaultInputAccessor`, `RestrictedConditionInputAccessor`, `DefaultLocalInputAccessor`。
*   `xyz.vvrf.reactor.dag.monitor`: DAG 监控相关的接口。
    *   `xyz.vvrf.reactor.dag.monitor.web.autoconfigure`: 监控模块的 Spring Boot 自动配置。
    *   `xyz.vvrf.reactor.dag.monitor.web.controller`: 监控相关的 WebFlux 控制器 (`DagMonitoringController`)。
    *   `xyz.vvrf.reactor.dag.monitor.web.dto`: 监控事件的数据传输对象 (`MonitoringEvent`)。
    *   `xyz.vvrf.reactor.dag.monitor.web.service`: 监控相关的服务 (`RealtimeDagMonitorListener`, `DagDefinitionCache`)。
*   `xyz.vvrf.reactor.dag.registry`: 节点注册相关的接口和实现，如 `NodeRegistry`, `SimpleNodeRegistry`, `SpringScanningNodeRegistry`。
*   `xyz.vvrf.reactor.dag.spring`: Spring 集成相关的通用类 (如 `EventAdapter`)。
*   `xyz.vvrf.reactor.dag.spring.boot`: 核心框架的 Spring Boot 自动配置 (`DagFrameworkAutoConfiguration`, `DagEngineProvider`, `DagFrameworkProperties`)。
*   `xyz.vvrf.reactor.dag.util`: 通用工具类 (如 `GraphUtils`)。

**关键文件作用:**

*   **`DagNode.java`**: 业务逻辑单元接口。
*   **`DagDefinitionBuilder.java`**: 构建 DAG 定义的入口。
*   **`StandardDagEngine.java`**: 核心 DAG 执行引擎。
*   **`StandardNodeExecutor.java`**: 核心单节点执行器。
*   **`NodeRegistry.java`**: 节点查找服务。
*   **`DagFrameworkAutoConfiguration.java`**: Spring Boot 自动配置主类。
*   **`DagMonitorWebAutoConfiguration.java`**: 监控模块的自动配置。
*   **`RealtimeDagMonitorListener.java`**: 实时事件监听和推送服务。
*   **`DagMonitoringController.java`**: 监控 API 端点。
*   **`index.html`**: 实时监控前端页面。
*   **`pom.xml`**: Maven 项目配置文件，定义依赖和构建过程。

## 运行测试 (Running Tests)

由于未提供具体的测试文件，这里提供通用指南：

1.  **单元测试**:
    *   针对单个 `DagNode` 实现的逻辑进行单元测试，可以使用 Mockito 模拟 `InputAccessor`。
    *   测试 `ConditionBase` 实现的评估逻辑。
    *   测试工具类如 `GraphUtils` 中的方法。
2.  **集成测试**:
    *   使用 `DagDefinitionBuilder` 构建小型的、特定的 DAG 定义。
    *   使用 `StandardDagEngine` (或通过 `DagEngineProvider` 获取) 来执行这些 DAG。
    *   使用 `reactor-test` 模块的 `StepVerifier` 来验证 `Flux<Event<?>>` 的输出、完成和错误信号。
    *   在 Spring Boot 环境中，可以使用 `@SpringBootTest` 来测试整个应用的集成，包括自动配置和 Bean 的注入。
3.  **测试覆盖范围**:
    *   不同类型的节点逻辑。
    *   各种边条件。
    *   `FAIL_FAST` 和 `CONTINUE_ON_FAILURE` 错误处理策略。
    *   节点超时和重试逻辑。
    *   监控事件的产生和捕获。

**执行 Maven 测试:**
```bash
mvn test
```

## API 文档链接 (API Reference)

*   **Javadoc**: (您需要自行生成 Javadoc)
    可以通过 Maven 命令生成 Javadoc:
    ```bash
    mvn javadoc:javadoc
    ```
    生成的文档通常位于 `target/site/apidocs/index.html`。建议将此文档部署到可访问的位置。

## 依赖项 (Dependencies)

主要外部依赖已在 [技术栈](#技术栈) 部分列出。核心依赖包括：

*   `io.projectreactor:reactor-core`
*   `org.slf4j:slf4j-api`
*   `org.projectlombok:lombok` (编译时)

对于 Spring Boot 集成和 Web 监控，还依赖：

*   `org.springframework.boot:spring-boot-autoconfigure`
*   `org.springframework:spring-context`
*   `org.springframework.boot:spring-boot-starter-validation`
*   `org.springframework:spring-webflux` (监控模块)
*   `com.fasterxml.jackson.core:jackson-databind` (监控模块)

详细列表请参见 `pom.xml` 文件。

## 贡献指南 (Contributing)

我们欢迎各种形式的贡献！

1.  **报告 Bug**: 如果您发现任何 Bug，请在项目的 GitHub Issues 中提交详细报告，包括复现步骤、期望行为和实际行为。
2.  **功能请求**: 如果您有新功能或改进建议，请也在 GitHub Issues 中提出。
3.  **代码贡献**:
    *   Fork 本仓库。
    *   创建一个新的特性分支 (e.g., `feature/my-new-feature` 或 `fix/issue-123`)。
    *   进行代码修改，确保遵循现有代码风格和约定。
    *   为您的修改添加适当的单元测试和集成测试。
    *   确保所有测试通过 (`mvn test`)。
    *   提交您的更改 (遵循清晰的 Commit Message 格式，例如 Conventional Commits)。
    *   将您的分支推送到您的 Fork。
    *   创建一个 Pull Request 到主仓库的 `main` (或 `develop`) 分支。
    *   在 Pull Request 中清晰描述您的更改。

**编码规范**:

*   遵循项目现有的代码风格。
*   添加必要的 Javadoc 注释，特别是公共 API。
*   编写清晰、可维护的代码。

## 版本历史 (Changelog/Release Notes)

请参考项目的 `CHANGELOG.md` 文件（如果存在）或 GitHub Releases 页面获取详细的版本历史和变更记录。

*(目前未提供 CHANGELOG.md，建议创建此文件以跟踪版本变更。)*

## 路线图 (Roadmap)

*(当前未提供明确的路线图。以下是一些可能的未来方向，具体需项目维护者确认)*

*   **更高级的调度策略**: 例如基于优先级的节点执行。
*   **持久化 DAG 状态与恢复**: 支持长时间运行的 DAG，在系统重启后能够恢复执行状态。
*   **可视化 DAG 构建器**: 一个图形化界面用于设计和构建 DAG 定义。
*   **更丰富的监控指标**: 集成 Micrometer 暴露更详细的性能指标。
*   **分布式 DAG 执行**: 支持将 DAG 的不同部分分发到不同服务实例上执行。
*   **更细致的输入/输出映射**: 支持更复杂的从多个上游槽到单个下游槽的数据聚合或转换逻辑。
*   **与更多消息队列/事件总线的集成**: 例如 Kafka, RabbitMQ。

## 许可证 (License)

本项目根据 [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.txt) 授权。
详细信息请参阅 `pom.xml` 中的 license 部分或项目根目录下的 `LICENSE` 文件（如果存在）。

## 常见问题解答 (FAQ)

**Q1: 如何处理节点执行的幂等性？**
A1: 框架本身不直接处理幂等性。节点实现者需要负责确保其业务逻辑是幂等的，尤其是在启用了重试策略时。

**Q2: 如何在节点之间传递大量数据？**
A2: 节点输出的 Payload 可以是任何 Java 对象。如果数据量非常大，建议考虑：
*   传递数据的引用（例如存储在外部系统中的 ID），而不是数据本身。
*   确保序列化和反序列化（如果跨进程）的开销是可接受的。
*   Reactor-DAG 主要用于编排，超大数据处理可能更适合专门的大数据处理框架。

**Q3: `StandardNodeExecutor` 中的 `CONFIG_KEY_RETRY` 和 `CONFIG_KEY_TIMEOUT` 如何使用？**
A3: 这两个常量键用于在 `DagDefinitionBuilder` 中为特定节点实例配置覆盖其默认或框架提供的超时和重试策略。
```java
// DagDefinitionBuilder context
builder.addNode("myNodeInstance", "myNodeTypeId");
builder.withTimeout("myNodeInstance", Duration.ofSeconds(5)); // 使用辅助方法
builder.withRetry("myNodeInstance", Retry.max(2));       // 使用辅助方法

    // 或者直接使用配置键 (不推荐，优先使用辅助方法)
    // builder.withConfiguration("myNodeInstance", StandardNodeExecutor.CONFIG_KEY_TIMEOUT, Duration.ofSeconds(5));
```

**Q4: 监控 UI (`index.html`) 无法显示我的 DAG 图，或者没有实时更新？**
A4: 请检查以下几点：
*   **后端服务运行正常**: 您的 Spring Boot 应用是否已启动且没有错误？
*   **WebFlux 环境**: 您的应用是否是 WebFlux 应用 (`spring-boot-starter-webflux` 是否在类路径中)？监控模块需要此环境。
*   **DAG Name 正确**: 您在监控页面输入的 DAG Name 是否与 `DagDefinitionBuilder` 中定义的完全一致？
*   **SSE 连接**: 浏览器开发者工具的网络(Network)标签页中，查看 `/dag-monitor/stream/{dagName}` 的 SSE 连接是否成功建立，是否有事件数据流过？
*   **Cytoscape.js 定义获取**: 查看 `/dag-monitor/dag-definition/{dagName}/cytoscapejs` 请求是否成功返回了有效的 JSON 数据？
*   **`DagDefinitionCache`**: 确保在 `DagDefinition` 构建完成后，其 Cytoscape.js JSON 表示通过 `DagDefinitionCache.cacheDag(...)` 被正确缓存。`MySimpleDag` 示例中已展示。
*   **浏览器兼容性**: 确保使用的浏览器支持 Vue.js 3 和 SSE。
*   **JavaScript 错误**: 检查浏览器开发者工具的控制台(Console)标签页是否有 JavaScript 错误。
*   **CORS 问题**: 如果监控页面与后端服务不在同域，可能需要配置 CORS。

**Q5: 如何自定义节点执行的线程池/调度器？**
A5: `DagFrameworkAutoConfiguration` 会创建一个名为 `dagNodeExecutionScheduler` 的 `Scheduler` Bean。您可以：
*   通过 `dag.scheduler.*` 属性配置其类型和参数。
*   或者，在您的 Spring 配置中提供一个自定义的 `Scheduler` Bean，并将其命名为 `dagNodeExecutionScheduler`，这样自动配置的默认 Bean 将不会创建。
*   如果 `dag.scheduler.type` 设置为 `CUSTOM`，并指定了 `dag.scheduler.custom-bean-name`，则框架会尝试从上下文中获取该名称的 `Scheduler` Bean。

**Q6: `SpringScanningNodeRegistry` 和手动注册节点可以混合使用吗？**
A6: `SpringScanningNodeRegistry` 内部委托给 `SimpleNodeRegistry`。它允许手动调用 `register` 方法，但会打印警告，因为推荐的方式是通过 `@DagNodeType` 注解进行自动扫描。混合使用可能导致管理上的混乱，建议选择一种主要方式。

## 致谢 (Acknowledgements)

*   感谢 [Project Reactor](https://projectreactor.io/) 团队提供了强大的反应式编程基础。
*   感谢 [Spring Boot](https://spring.io/projects/spring-boot) 团队简化了 Java 应用的开发和配置。
*   感谢 [Cytoscape.js](https://js.cytoscape.org/) 社区提供了优秀的图形可视化库。
*   感谢 [Vue.js](https://vuejs.org/) 社区。

## 联系方式/支持 (Contact/Support)

*   **GitHub Issues**: 如有 Bug 或功能请求，请通过项目 GitHub 仓库的 [Issues](https://github.com/vvrf/reactor-dag/issues) 页面提交。 <!-- 请替换为实际的仓库链接 -->
*   **开发者邮箱**: `vvrfxyz@gmail.com` 

