package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.DependencyAccessor; // Import Accessor
// Removed DependencyDescriptor import (not used here)
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.List;
// Removed Map import
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * reactor-dag
 * The final node, depends on all parallel nodes. Acts as a sync point.
 * Uses DependencyAccessor.
 *
 * @author Your Name (modified)
 * @date Today's Date (modified)
 */
@Component
public class FinalNode implements DagNode<ParalleContext, String, Void> {

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    @Override
    public Class<Void> getEventType() {
        return Void.class;
    }

    // Define the names of the expected dependencies explicitly
    private static final List<String> EXPECTED_DEPENDENCIES = List.of("ParallelNodeA", "ParallelNodeB", "ParallelNodeC");

    /**
     * Executes the final node logic, aggregating results from parallel nodes.
     *
     * @param context      The parallel context.
     * @param dependencies Accessor for dependency results. <--- Updated Javadoc
     * @return A Mono containing the final result.
     */
    @Override
    public Mono<NodeResult<ParalleContext, String, Void>> execute(ParalleContext context, DependencyAccessor<ParalleContext> dependencies) { // <--- Signature changed
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + getName() + " on thread: " + threadName);

            // Aggregate results using the DependencyAccessor
            String aggregatedPayloads = EXPECTED_DEPENDENCIES.stream()
                    .map(depName -> {
                        // Safely get payload using accessor, provide default if absent or failed
                        String payloadStr = dependencies.getPayload(depName, String.class) // Assuming String payload
                                .orElseGet(() -> {
                                    // Check if the dependency actually failed vs just having no payload
                                    boolean failed = !dependencies.isSuccess(depName);
                                    return failed ? "FAILED" : "EMPTY";
                                });
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
                        context, e, this);
            }

            String resultPayload = getName() + " finished successfully on " + threadName + ". Aggregated: " + aggregatedPayloads;
            System.out.println(getName() + " finished.");

            return NodeResult.success(context, resultPayload, this);
        }).onErrorResume(error -> {
            System.err.println("Error executing " + getName() + ": " + error.getMessage());
            return Mono.just(NodeResult.<ParalleContext, String, Void>failure(
                    context, error, this));
        });
    }
}
