//package xyz.vvrf.reactor.dag.example.dataProcessingDag.node;
//
///**
// * reactor-dag-example
// *
// * @author ruifeng.wen
// * @date 2025/4/2
// */
//
//import lombok.extern.slf4j.Slf4j;
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Mono;
//import xyz.vvrf.reactor.dag.core.DagNode;
//import xyz.vvrf.reactor.dag.core.DependencyDescriptor;
//import xyz.vvrf.reactor.dag.core.Event;
//import xyz.vvrf.reactor.dag.core.NodeResult;
//import xyz.vvrf.reactor.dag.example.dataProcessingDag.DataItem;
//import xyz.vvrf.reactor.dag.example.dataProcessingDag.ProcessingContext;
//
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
///**
// * 数据富集节点 - 为数据添加额外信息
// */
//@Slf4j
//public class DataEnrichmentNode implements DagNode<ProcessingContext, List<DataItem>> {
//
//    @Override
//    public String getName() {
//        return "dataEnrichment";
//    }
//
//    @Override
//    public List<DependencyDescriptor> getDependencies() {
//        List<DependencyDescriptor> deps = new ArrayList<>();
//        deps.add(new DependencyDescriptor("dataTransformation", List.class));
//        return deps;
//    }
//
//    @Override
//    @SuppressWarnings("unchecked")
//    public Class<List<DataItem>> getPayloadType() {
//        return (Class<List<DataItem>>) (Class<?>) List.class;
//    }
//
//    @Override
//    public Mono<NodeResult<ProcessingContext, List<DataItem>>> execute(
//            ProcessingContext context, Map<String, NodeResult<ProcessingContext, ?>> dependencyResults) {
//
//        return Mono.fromCallable(() -> {
//            if (!context.isIncludeEnrichment()) {
//                // 如果不需要富集，直接返回转换后的数据
//                context.setEnrichedData(context.getTransformedData());
//                return new NodeResult<>(context, Flux.empty(), getPayloadType());
//            }
//
//            List<DataItem> enrichedItems = new ArrayList<>();
//
//            for (DataItem item : context.getTransformedData()) {
//                // 模拟数据富集
//                item.setEnrichedInfo("加强信息: " + item.getId());
//                enrichedItems.add(item);
//            }
//
//            context.setEnrichedData(enrichedItems);
//
//            // 使用一致的泛型类型修复事件创建
//            Flux<Event<List<DataItem>>> events = Flux.concat(
//                    Mono.just(Event.of("enrich.start", enrichedItems)),
//                    Mono.just(Event.of("enrich.progress", enrichedItems)),
//                    Mono.just(Event.of("enrich.complete", enrichedItems))
//            ).delayElements(Duration.ofMillis(300)); // 模拟延迟
//
//            return new NodeResult<>(context, events, getPayloadType());
//        });
//    }
//}
//
