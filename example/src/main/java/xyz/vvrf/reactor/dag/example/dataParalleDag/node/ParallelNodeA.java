package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

/**
 * reactor-dag
 *
 * @author ruifeng.wen
 * @date 4/19/25
 */

import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.DependencyDescriptor;
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * reactor-dag
 * A node designed to run in parallel with other similar nodes.
 * Depends on FirstNode.
 *
 * @author Your Name
 * @date Today's Date
 */
@Component
public class ParallelNodeA implements DagNode<ParalleContext, String> {

    @Override
    public String getName() {
        return "ParallelNodeA";
    }

    @Override
    public List<DependencyDescriptor> getDependencies() {
        // Depends on FirstNode
        return List.of(new DependencyDescriptor("FirstNode", String.class));
    }

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, Map<String, NodeResult<ParalleContext, ?>> dependencyResults) {
        // Access dependency result if needed
        // NodeResult<ParalleContext, ?> firstNodeResult = dependencyResults.get("FirstNode");
        // String firstNodePayload = (String) firstNodeResult.getPayload(); // Assuming getPayload() exists
        // System.out.println(getName() + " received payload from FirstNode: " + firstNodePayload);

        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + getName() + " on thread: " + threadName + " (depends on FirstNode)");

            // Simulate significant work
            try {
                TimeUnit.MILLISECONDS.sleep(500); // Longer delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // In a real app, return Mono.error or a failed NodeResult
                throw new RuntimeException(e);
            }

            String resultPayload = getName() + " executed successfully on " + threadName;
            System.out.println(getName() + " finished.");
            return new NodeResult<>(context, Flux.empty(),String.class);
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
