package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
// Removed unused Schedulers import
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
 * @author Your Name (modified)
 * @date Today's Date (modified)
 */
@Component
public class ParallelNodeA implements DagNode<ParalleContext, String, Void> { // Added Void event type

    @Override
    public String getName() {
        return "ParallelNodeA";
    }

    @Override
    public List<DependencyDescriptor> getDependencies() {
        return List.of(new DependencyDescriptor("FirstNode", String.class));
    }

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    @Override
    public Class<Void> getEventType() {
        return  Void.class;
    }

    @Override
    public Mono<NodeResult<ParalleContext, String, Void>> execute(ParalleContext context, Map<String, NodeResult<ParalleContext, ?, ?>> dependencyResults) {
        // Example of accessing dependency (optional)
        dependencyResults.get("FirstNode")
                .getPayload() // Access Optional<Payload>
                .ifPresent(payload -> System.out.println(getName() + " received payload from FirstNode: " + payload));

        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + getName() + " on thread: " + threadName + " (depends on FirstNode)");

            // Simulate significant work
            try {
                TimeUnit.MILLISECONDS.sleep(500); // Longer delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return NodeResult.<ParalleContext, String, Void>failure(
                        context, e, String.class, Void.class);
            }

            String resultPayload = getName() + " executed successfully on " + threadName;
            System.out.println(getName() + " finished.");

            // Use the correct static factory method
            return NodeResult.success(context, resultPayload, String.class, Void.class);
        }).onErrorResume(error -> {
            System.err.println("Error executing " + getName() + ": " + error.getMessage());
            return Mono.just(NodeResult.<ParalleContext, String, Void>failure(
                    context, error, String.class, Void.class));
        });
    }
}
