package xyz.vvrf.reactor.dag.example;

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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据验证节点 - 验证数据的有效性
 */
@Slf4j
class DataValidationNode implements DagNode<ProcessingContext, List<DataItem>> {

    @Override
    public String getName() {
        return "dataValidation";
    }

    @Override
    public List<DependencyDescriptor> getDependencies() {
        List<DependencyDescriptor> deps = new ArrayList<>();
        deps.add(new DependencyDescriptor("dataFetch", List.class));
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
            List<DataItem> validatedItems = new ArrayList<>();
            int validCount = 0;

            for (DataItem item : context.getRawData()) {
                if (item.isValid()) {
                    validatedItems.add(item);
                    validCount++;
                }
            }

            context.setValidatedData(validatedItems);

            // 使用一致的泛型类型修复事件创建
            Flux<Event<List<DataItem>>> events = Flux.concat(
                    Mono.just(Event.of("validation.start", validatedItems)),
                    Mono.just(Event.of("validation.stats", validatedItems)),
                    Mono.just(Event.of("validation.complete", validatedItems))
            ).delayElements(Duration.ofMillis(200)); // 模拟延迟

            return new NodeResult<>(context, events, getPayloadType());
        });
    }
}

