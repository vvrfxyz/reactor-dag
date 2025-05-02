//// [file name]: TestDependencyAccessor.java
//package xyz.vvrf.reactor.dag.test.util;
//
//import reactor.core.publisher.Flux;
//import xyz.vvrf.reactor.dag.core.DependencyAccessor;
//import xyz.vvrf.reactor.dag.core.Event;
//import xyz.vvrf.reactor.dag.core.NodeResult;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Objects;
//import java.util.Optional;
//
///**
// * 用于测试的 DependencyAccessor 实现。
// * 允许手动设置依赖节点的结果。
// *
// * @param <C> 上下文类型
// */
//public class TestDependencyAccessor<C> implements DependencyAccessor<C> {
//
//    private final Map<String, NodeResult<C, ?, ?>> results = new HashMap<>();
//
//    /**
//     * 添加或替换一个依赖节点的结果。
//     *
//     * @param dependencyName 依赖节点名称
//     * @param result         该依赖节点的 NodeResult
//     * @return this 实例，支持链式调用
//     */
//    public TestDependencyAccessor<C> addResult(String dependencyName, NodeResult<C, ?, ?> result) {
//        Objects.requireNonNull(dependencyName, "依赖名称不能为空");
//        Objects.requireNonNull(result, "依赖结果不能为空 for " + dependencyName);
//        this.results.put(dependencyName, result);
//        return this;
//    }
//
//    /**
//     * 添加一个成功的依赖结果，只包含 Payload。
//     * 需要显式提供类型信息。
//     */
//    public <P, T_Event> TestDependencyAccessor<C> addSuccessResult(String dependencyName, C context, P payload, Class<P> payloadType, Class<T_Event> eventType) {
//        NodeResult<C, P, T_Event> result = NodeResult.success(context, payload, payloadType, eventType);
//        return addResult(dependencyName, result);
//    }
//
//    /**
//     * 添加一个失败的依赖结果。
//     * 需要显式提供类型信息。
//     */
//    public <P, T_Event> TestDependencyAccessor<C> addFailureResult(String dependencyName, C context, Throwable error, Class<P> payloadType, Class<T_Event> eventType) {
//        NodeResult<C, P, T_Event> result = NodeResult.failure(context, error, payloadType, eventType);
//        return addResult(dependencyName, result);
//    }
//
//    /**
//     * 添加一个跳过的依赖结果。
//     * 需要显式提供类型信息。
//     */
//    public <P, T_Event> TestDependencyAccessor<C> addSkippedResult(String dependencyName, C context, Class<P> payloadType, Class<T_Event> eventType) {
//        NodeResult<C, P, T_Event> result = NodeResult.skipped(context, payloadType, eventType);
//        return addResult(dependencyName, result);
//    }
//
//
//    @Override
//    public Optional<NodeResult<C, ?, ?>> getResult(String dependencyName) {
//        return Optional.ofNullable(results.get(dependencyName));
//    }
//
//    @Override
//    public <DepP> Optional<DepP> getPayload(String dependencyName, Class<DepP> expectedType) {
//        Objects.requireNonNull(expectedType, "Expected payload type cannot be null");
//        return getResult(dependencyName)
//                .filter(NodeResult::isSuccess)
//                .flatMap(NodeResult::getPayload)
//                .filter(expectedType::isInstance)
//                .map(expectedType::cast);
//    }
//
//    @Override
//    @SuppressWarnings("unchecked") // 类型转换是安全的
//    public Flux<Event<?>> getEvents(String dependencyName) {
//        return getResult(dependencyName)
//                .filter(NodeResult::isSuccess)
//                .map(result -> (Flux<Event<?>>) (Flux<? extends Event<?>>) result.getEvents()) // 安全转换
//                .orElse(Flux.empty());
//    }
//
//    @Override
//    public boolean isSuccess(String dependencyName) {
//        return getResult(dependencyName).map(NodeResult::isSuccess).orElse(false);
//    }
//
//    @Override
//    public boolean isFailure(String dependencyName) {
//        return getResult(dependencyName).map(NodeResult::isFailure).orElse(false);
//    }
//
//    @Override
//    public boolean isSkipped(String dependencyName) {
//        return getResult(dependencyName).map(NodeResult::isSkipped).orElse(false);
//    }
//
//    @Override
//    public boolean contains(String dependencyName) {
//        return results.containsKey(dependencyName);
//    }
//}
