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
 * 结果组装节点 - 组装最终处理结果
 */
@Slf4j
class ResultAssemblyNode implements DagNode<ProcessingContext, ProcessingResult> {

    @Override
    public String getName() {
        return "resultAssembly";
    }

    @Override
    public List<DependencyDescriptor> getDependencies() {
        List<DependencyDescriptor> deps = new ArrayList<>();
        deps.add(new DependencyDescriptor("dataEnrichment", List.class));
        return deps;
    }

    @Override
    public Class<ProcessingResult> getPayloadType() {
        return ProcessingResult.class;
    }

    @Override
    public Mono<NodeResult<ProcessingContext, ProcessingResult>> execute(
            ProcessingContext context, Map<String, NodeResult<ProcessingContext, ?>> dependencyResults) {

        return Mono.fromCallable(() -> {
            ProcessingResult result = new ProcessingResult();
            result.setTotalItems(context.getRawData().size());
            result.setValidItems(context.getValidatedData().size());
            result.setTransformedItems(context.getTransformedData().size());
            result.setEnrichedItems(context.getEnrichedData().size());
            result.setProcessingTimeMs(System.currentTimeMillis());

            context.setResult(result);

            // 创建事件流
            Flux<Event<ProcessingResult>> events = Flux.just(
                    Event.of("result.complete", result)
            ).delayElements(Duration.ofMillis(100)); // 模拟延迟

            return new NodeResult<>(context, events, getPayloadType());
        });
    }
}