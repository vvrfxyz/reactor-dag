// file: example/dataParalleDag/node/FirstNode.java
package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

// Removed @Component
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.InputAccessor; // Use InputAccessor
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 起始节点逻辑。
 * 不再是 Spring 组件。
 * 使用 InputAccessor (虽然此节点无输入)。
 */
public class FirstNode implements DagNode<ParalleContext, String> { // Removed Event Type <Void>

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    // No inputs required
    @Override
    public Map<String, Class<?>> getInputRequirements() {
        return Collections.emptyMap();
    }

    /**
     * 执行起始节点逻辑。
     *
     * @param context 上下文
     * @param inputs  输入访问器 (此节点未使用)
     * @return 结果 Mono
     */
    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, InputAccessor<ParalleContext> inputs) { // Use InputAccessor
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + this.getClass().getSimpleName() + " logic on thread: " + threadName);

            try {
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Pass payloadType explicitly
                return NodeResult.failure(context, e, String.class);
            }

            String resultPayload = this.getClass().getSimpleName() + " executed successfully on " + threadName;
            System.out.println(this.getClass().getSimpleName() + " logic finished.");

            // Pass payloadType explicitly
            return NodeResult.success(context, resultPayload, String.class);
        }).onErrorResume(error -> {
            System.err.println("Error executing " + this.getClass().getSimpleName() + " logic: " + error.getMessage());
            // Pass payloadType explicitly
            return Mono.just(NodeResult.failure(context, error, String.class));
        });
    }
}
