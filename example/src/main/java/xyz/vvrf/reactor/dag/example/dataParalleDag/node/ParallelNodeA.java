// file: example/dataParalleDag/node/ParallelNodeA.java
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
 * 并行节点 A 的逻辑。
 * 声明需要一个名为 "startData" 的 String 类型输入。
 */
public class ParallelNodeA implements DagNode<ParalleContext, String> { // Removed Event Type <Void>

    private static final String INPUT_SLOT_NAME = "startData"; // Define logical input slot name

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    // Declare input requirement
    @Override
    public Map<String, Class<?>> getInputRequirements() {
        return Map.of(INPUT_SLOT_NAME, String.class); // Requires a String input named "startData"
    }

    /**
     * 执行并行节点 A 的逻辑。
     *
     * @param context 上下文
     * @param inputs  输入访问器
     * @return 结果 Mono
     */
    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, InputAccessor<ParalleContext> inputs) { // Use InputAccessor

        // Access input using the logical slot name
        inputs.getPayload(INPUT_SLOT_NAME, String.class)
                .ifPresent(payload -> System.out.println(this.getClass().getSimpleName() + " logic received payload from input '" + INPUT_SLOT_NAME + "': " + payload));

        // Check if the input was available and successful (more robust check)
        if (!inputs.isInputAvailable(INPUT_SLOT_NAME)) {
            // Check if it failed or was skipped
            if (inputs.isInputFailed(INPUT_SLOT_NAME)) {
                System.out.println(this.getClass().getSimpleName() + " notes that input '" + INPUT_SLOT_NAME + "' failed.");
            } else if (inputs.isInputSkipped(INPUT_SLOT_NAME)) {
                System.out.println(this.getClass().getSimpleName() + " notes that input '" + INPUT_SLOT_NAME + "' was skipped.");
            } else {
                System.out.println(this.getClass().getSimpleName() + " notes that input '" + INPUT_SLOT_NAME + "' was not available (no payload).");
            }
            // Potentially alter behavior or return skipped/failure
        }

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
