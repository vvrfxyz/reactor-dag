// [file name]: finalnode.java
package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.DependencyDescriptor;
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * reactor-dag
 * The final node, depends on all parallel nodes. Acts as a sync point.
 *
 * @author Your Name
 * @date Today's Date
 */
@Component
public class FinalNode implements DagNode<ParalleContext, String> {

    @Override
    public String getName() {
        return "FinalNode";
    }

    @Override
    public List<DependencyDescriptor> getDependencies() {
        // Depends on all three parallel nodes
        return List.of(
                new DependencyDescriptor("ParallelNodeA", String.class),
                new DependencyDescriptor("ParallelNodeB", String.class),
                new DependencyDescriptor("ParallelNodeC", String.class)
        );
    }

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, Map<String, NodeResult<ParalleContext, ?>> dependencyResults) {
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + getName() + " on thread: " + threadName);

            // Aggregate results from dependencies (optional)
            String aggregatedPayloads = dependencyResults.entrySet().stream()
                    .map(entry -> entry.getKey() + " result: [" + entry.getValue()+ "]") // Assuming getPayload()
                    .collect(Collectors.joining("; "));

            System.out.println(getName() + " received aggregated results: " + aggregatedPayloads);

            // Simulate final processing
            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            String resultPayload = getName() + " finished successfully on " + threadName;
            System.out.println(getName() + " finished.");
            return new NodeResult<>(context, Flux.empty(),String.class);
        });
    }
}
