package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.DependencyAccessor;
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.concurrent.TimeUnit;

/**
 * reactor-dag
 * The starting node of the DAG.
 * Uses DependencyAccessor (though doesn't access dependencies).
 *
 * @author ruifeng.wen
 * @date 4/19/25 (modified)
 */
@Component
public class FirstNode implements DagNode<ParalleContext, String, Void> {

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    @Override
    public Class<Void> getEventType() {
        return Void.class;
    }

    /**
     * Executes the first node logic.
     *
     * @param context      The parallel context.
     * @param dependencies Accessor for dependency results (unused in this node). <--- Updated Javadoc
     * @return A Mono containing the result.
     */
    @Override
    public Mono<NodeResult<ParalleContext, String, Void>> execute(ParalleContext context, DependencyAccessor<ParalleContext> dependencies) {
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + getName() + " on thread: " + threadName);

            // Simulate some work
            try {
                TimeUnit.MILLISECONDS.sleep(50); // Small delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return NodeResult.failure(context, e, this);
            }

            String resultPayload = getName() + " executed successfully on " + threadName;
            System.out.println(getName() + " finished.");

            return NodeResult.success(context, resultPayload, this);
        }).onErrorResume(error -> {
            System.err.println("Error executing " + getName() + ": " + error.getMessage());
            return Mono.just(NodeResult.<ParalleContext, String, Void>failure(
                    context, error, this));
        });
    }
}
