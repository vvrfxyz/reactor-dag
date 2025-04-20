// [file name]: finalnode.java
package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import org.springframework.stereotype.Component;
// Removed unused Flux import
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
 * @author Your Name (modified)
 * @date Today's Date (modified)
 */
@Component
public class FinalNode implements DagNode<ParalleContext, String, Void> { // Added Void event type

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
    public Class<Void> getEventType() {
        return Void.class;
    }

    @Override
    public Mono<NodeResult<ParalleContext, String, Void>> execute(ParalleContext context, Map<String, NodeResult<ParalleContext, ?, ?>> dependencyResults) {
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + getName() + " on thread: " + threadName);

            // Aggregate results from dependencies correctly
            String aggregatedPayloads = dependencyResults.entrySet().stream()
                    .map(entry -> {
                        String depName = entry.getKey();
                        NodeResult<ParalleContext, ?, ?> depResult = entry.getValue();
                        // Safely get payload, provide default if absent or failed
                        String payloadStr = depResult.getPayload()
                                .map(Object::toString) // Convert payload to string
                                .orElseGet(() -> depResult.isFailure() ? "FAILED" : "EMPTY"); // Indicate failure or empty
                        return depName + ": [" + payloadStr + "]";
                    })
                    .collect(Collectors.joining("; "));

            System.out.println(getName() + " received aggregated results: " + aggregatedPayloads);

            // Simulate final processing
            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return NodeResult.<ParalleContext, String, Void>failure(
                        context, e, String.class, Void.class);
            }

            String resultPayload = getName() + " finished successfully on " + threadName + ". Aggregated: " + aggregatedPayloads;
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
