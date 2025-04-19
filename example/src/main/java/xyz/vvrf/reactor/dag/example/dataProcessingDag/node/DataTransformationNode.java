package xyz.vvrf.reactor.dag.example.dataProcessingDag.node;

/**
 * reactor-dag-example
 *
 * @author ruifeng.wen
 * @date 2025/4/2
 */

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.DependencyDescriptor;
import xyz.vvrf.reactor.dag.core.Event;
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataProcessingDag.DataItem;
import xyz.vvrf.reactor.dag.example.dataProcessingDag.ProcessingContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据转换节点 - 对数据进行转换处理
 */
@Slf4j
public class DataTransformationNode implements DagNode<ProcessingContext, List<DataItem>> {

    @Override
    public String getName() {
        return "dataTransformation";
    }

    @Override
    public List<DependencyDescriptor> getDependencies() {
        List<DependencyDescriptor> deps = new ArrayList<>();
        deps.add(new DependencyDescriptor("dataValidation", List.class));
        return deps;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<List<DataItem>> getPayloadType() {
        return (Class<List<DataItem>>) (Class<?>) List.class;
    }

    @Override
    public Mono<NodeResult<ProcessingContext, List<DataItem>>> execute(
            ProcessingContext context, Map<String, NodeResult<ProcessingContext, ?>> dependencyResults) {

        return Mono.fromCallable(() -> {
            List<DataItem> transformedItems = new ArrayList<>();

            for (DataItem item : context.getValidatedData()) {
                // 模拟数据转换
                item.setValue(item.getValue().toUpperCase());
                transformedItems.add(item);
            }

            context.setTransformedData(transformedItems);

            // 使用一致的泛型类型修复事件创建
            Flux<Event<List<DataItem>>> events = Flux.concat(
                    Mono.just(Event.of("transform.start", transformedItems)),
                    Mono.just(Event.of("transform.progress", transformedItems)),
                    Mono.just(Event.of("transform.complete", transformedItems))
            ).delayElements(Duration.ofMillis(250)); // 模拟延迟

            return new NodeResult<>(context, events, getPayloadType());
        });
    }
}
