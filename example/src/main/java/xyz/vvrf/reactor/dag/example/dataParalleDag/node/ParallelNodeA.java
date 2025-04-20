package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.DependencyAccessor; // Import Accessor
// Removed DependencyDescriptor import
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

// Removed List import
// Removed Map import
import java.util.concurrent.TimeUnit;

/**
 * reactor-dag
 * A node designed to run in parallel with other similar nodes.
 * Depends on FirstNode.
 * Uses DependencyAccessor.
 *
 * @author Your Name (modified)
 * @date Today's Date (modified)
 */
@Component
public class ParallelNodeA implements DagNode<ParalleContext, String, Void> {

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    @Override
    public Class<Void> getEventType() {
        return Void.class;
    }

    /**
     * Executes the parallel node A logic.
     *
     * @param context      The parallel context.
     * @param dependencies Accessor for dependency results. <--- Updated Javadoc
     * @return A Mono containing the result.
     */
    @Override
    public Mono<NodeResult<ParalleContext, String, Void>> execute(ParalleContext context, DependencyAccessor<ParalleContext> dependencies) { // <--- Signature changed

        // Example of accessing dependency using the accessor
        dependencies.getPayload("FirstNode", String.class) // Use accessor with type safety
                .ifPresent(payload -> System.out.println(getName() + " received payload from FirstNode: " + payload));

        // Check if FirstNode succeeded (optional)
        if (!dependencies.isSuccess("FirstNode")) {
            System.out.println(getName() + " notes that FirstNode did not succeed.");
            // Potentially alter behavior based on dependency failure
        }

        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + getName() + " on thread: " + threadName + " (depends on FirstNode)");

            // Simulate significant work
            try {
                TimeUnit.MILLISECONDS.sleep(500); // Longer delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return NodeResult.failure(
                        context, e, this);
            }

            String resultPayload = getName() + " executed successfully on " + threadName;
            System.out.println(getName() + " finished.");

            return NodeResult.success(context, resultPayload, this);
        }).onErrorResume(error -> {
            System.err.println("Error executing " + getName() + ": " + error.getMessage());
            return Mono.just(NodeResult.failure(
                    context, error, this));
        });
    }
}
