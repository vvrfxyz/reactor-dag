// [file name]: firstnode.java
package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DependencyDescriptor;
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;
import xyz.vvrf.reactor.dag.core.DagNode;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit; // Import TimeUnit

/**
 * reactor-dag
 * The starting node of the DAG.
 *
 * @author ruifeng.wen
 * @date 4/19/25
 */
@Component
public class FirstNode implements DagNode<ParalleContext, String> {

    @Override
    public String getName() {
        // Use simple name for dependency matching
        return "FirstNode";
    }

    @Override
    public List<DependencyDescriptor> getDependencies() {
        // No dependencies, this is the root node
        return Collections.emptyList();
    }

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, Map<String, NodeResult<ParalleContext, ?>> dependencyResults) {
        // Use Mono.fromCallable to wrap potentially blocking code or simple computations
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + getName() + " on thread: " + threadName);

            // Simulate some work
            try {
                TimeUnit.MILLISECONDS.sleep(50); // Small delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e); // Or handle more gracefully
            }

            String resultPayload = getName() + " executed successfully on " + threadName;
            System.out.println(getName() + " finished.");

            // Assuming NodeResult has a constructor like this or a static factory method
            // Adjust based on the actual NodeResult implementation in reactor-dag
            return new NodeResult<>(context, Flux.empty(),String.class); // Or NodeResult.success(context, resultPayload);
        });
        // If the work is inherently non-blocking/reactive, you would construct
        // the Mono differently, e.g., calling another reactive service.
        // For this example, fromCallable is suitable for simulating work with sleep.
    }
}
