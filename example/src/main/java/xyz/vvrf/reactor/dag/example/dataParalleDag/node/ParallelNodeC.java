// [file name]: parallelnodec.java
package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

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
public class ParallelNodeC implements DagNode<ParalleContext, String> {

    @Override
    public String getName() {
        return "ParallelNodeC";
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
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + getName() + " on thread: " + threadName + " (depends on FirstNode)");

            // Simulate significant work
            try {
                TimeUnit.MILLISECONDS.sleep(500); // Longer delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            String resultPayload = getName() + " executed successfully on " + threadName;
            System.out.println(getName() + " finished.");
            return new NodeResult<>(context, Flux.empty(),String.class);
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
