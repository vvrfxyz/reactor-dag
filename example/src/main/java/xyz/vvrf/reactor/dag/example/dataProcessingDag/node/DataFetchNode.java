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
import java.util.UUID;

/**
 * 数据获取节点 - 负责从指定来源获取原始数据
 */
@Slf4j
public class DataFetchNode implements DagNode<ProcessingContext, List<DataItem>> {

    @Override
    public String getName() {
        return "dataFetch";
    }

    @Override
    public List<DependencyDescriptor> getDependencies() {
        return new ArrayList<>(); // 无依赖节点
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
            // 模拟数据获取过程
            List<DataItem> items = new ArrayList<>();
            for (int i = 0; i < context.getDataSize(); i++) {
                DataItem item = new DataItem();
                item.setId(UUID.randomUUID().toString());
                item.setValue("Value-" + i);
                item.setValid(Math.random() > 0.1); // 10%的数据无效
                items.add(item);
            }

            context.setRawData(items);

            // 使用一致的泛型类型修复事件创建
            Flux<Event<List<DataItem>>> events = Flux.concat(
                    Mono.just(Event.of("fetch.start", items)),
                    Mono.just(Event.of("fetch.progress", items.subList(0, Math.min(10, items.size())))),
                    Mono.just(Event.of("fetch.complete", items))
            ).delayElements(Duration.ofMillis(300)); // 模拟延迟

            return new NodeResult<>(context, events, getPayloadType());
        });
    }
}
