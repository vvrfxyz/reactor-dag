// [file name]: firstnode.java
package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DagNode; // Correct import
import xyz.vvrf.reactor.dag.core.DependencyDescriptor;
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit; // Import TimeUnit

/**
 * reactor-dag
 * The starting node of the DAG.
 *
 * @author ruifeng.wen
 * @date 4/19/25 (modified)
 */
@Component
public class FirstNode implements DagNode<ParalleContext, String, Void> { // Added Void event type

    @Override
    public String getName() {
        return "FirstNode";
    }

    @Override
    public List<DependencyDescriptor> getDependencies() {
        return Collections.emptyList();
    }

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    @Override
    public Class<Void> getEventType() {
        return Void.class;
    }

    // Implicitly getEventType() would be Void.class if needed by NodeResult internally

    @Override
    public Mono<NodeResult<ParalleContext, String, Void>> execute(ParalleContext context, Map<String, NodeResult<ParalleContext, ?, ?>> dependencyResults) {
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + getName() + " on thread: " + threadName);

            // Simulate some work
            try {
                TimeUnit.MILLISECONDS.sleep(50); // Small delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Return a failure result instead of throwing RuntimeException in reactive chain
                return NodeResult.failure(context, e, String.class, Void.class);
            }

            String resultPayload = getName() + " executed successfully on " + threadName;
            System.out.println(getName() + " finished.");

            // Use the correct static factory method
            return NodeResult.success(context, resultPayload, String.class,Void.class);
        }).onErrorResume(error -> {
            // Handle errors from fromCallable (like the RuntimeException if sleep was interrupted before fix)
            System.err.println("Error executing " + getName() + ": " + error.getMessage());
            return Mono.just(NodeResult.<ParalleContext, String, Void>failure(
                    context, error, String.class,Void.class));
        });
    }
}
