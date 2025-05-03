// file: example/dataParalleDag/node/ParallelNodeC.java
package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

// Removed @Component
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.InputAccessor; // Use InputAccessor
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 并行节点 C 的逻辑。
 * 声明需要一个名为 "startData" 的 String 类型输入 (即使未使用)。
 */
public class ParallelNodeC implements DagNode<ParalleContext, String> { // Removed Event Type <Void>

    private static final String INPUT_SLOT_NAME = "startData";

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    // Declare input requirement
    @Override
    public Map<String, Class<?>> getInputRequirements() {
        return Map.of(INPUT_SLOT_NAME, String.class);
    }

    /**
     * 执行并行节点 C 的逻辑。
     *
     * @param context 上下文
     * @param inputs  输入访问器 (此示例未使用其数据)
     * @return 结果 Mono
     */
    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, InputAccessor<ParalleContext> inputs) { // Use InputAccessor
        // inputs parameter is available but not used to retrieve data in this specific logic
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + this.getClass().getSimpleName() + " logic on thread: " + threadName + " (requires input '" + INPUT_SLOT_NAME + "')");

            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return NodeResult.failure(context, e, String.class); // Pass payloadType
            }

            String resultPayload = this.getClass().getSimpleName() + " executed successfully on " + threadName;
            System.out.println(this.getClass().getSimpleName() + " logic finished.");

            return NodeResult.success(context, resultPayload, String.class); // Pass payloadType
        }).onErrorResume(error -> {
            System.err.println("Error executing " + this.getClass().getSimpleName() + " logic: " + error.getMessage());
            return Mono.just(NodeResult.failure(context, error, String.class)); // Pass payloadType
        });
    }
}
