// file: example/dataParalleDag/node/FinalNode.java
package xyz.vvrf.reactor.dag.example.dataParalleDag.node;

// Removed @Component
import reactor.core.publisher.Mono;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.InputAccessor; // Use InputAccessor
import xyz.vvrf.reactor.dag.core.NodeResult;
import xyz.vvrf.reactor.dag.example.dataParalleDag.ParalleContext;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 最终节点逻辑，聚合来自多个并行输入的结果。
 * 声明需要 "resultA", "resultB", "resultC" 三个 String 输入。
 */
public class FinalNode implements DagNode<ParalleContext, String> { // Removed Event Type <Void>

    // Define logical input slot names
    private static final String INPUT_A = "resultA";
    private static final String INPUT_B = "resultB";
    private static final String INPUT_C = "resultC";

    @Override
    public Class<String> getPayloadType() {
        return String.class;
    }

    // Declare multiple input requirements
    @Override
    public Map<String, Class<?>> getInputRequirements() {
        return Map.of(
                INPUT_A, String.class,
                INPUT_B, String.class,
                INPUT_C, String.class
        );
    }

    /**
     * 执行最终节点逻辑，聚合结果。
     *
     * @param context 上下文
     * @param inputs  输入访问器
     * @return 结果 Mono
     */
    @Override
    public Mono<NodeResult<ParalleContext, String>> execute(ParalleContext context, InputAccessor<ParalleContext> inputs) { // Use InputAccessor
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Executing " + this.getClass().getSimpleName() + " logic on thread: " + threadName);

            // Aggregate results using InputAccessor and declared slot names
            String aggregatedPayloads = getInputRequirements().keySet().stream() // Iterate over declared input slots
                    .map(inputSlotName -> {
                        String payloadStr = inputs.getPayload(inputSlotName, String.class)
                                .orElseGet(() -> {
                                    if (inputs.isInputFailed(inputSlotName)) return "FAILED";
                                    if (inputs.isInputSkipped(inputSlotName)) return "SKIPPED";
                                    return "EMPTY"; // Input available but no payload (or not available)
                                });
                        return inputSlotName + ": [" + payloadStr + "]"; // Use slot name in output
                    })
                    .collect(Collectors.joining("; "));

            System.out.println(this.getClass().getSimpleName() + " logic received aggregated results: " + aggregatedPayloads);

            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return NodeResult.failure(context, e, String.class); // Pass payloadType
            }

            String resultPayload = this.getClass().getSimpleName() + " finished successfully on " + threadName + ". Aggregated: " + aggregatedPayloads;
            System.out.println(this.getClass().getSimpleName() + " logic finished.");

            return NodeResult.success(context, resultPayload, String.class); // Pass payloadType
        }).onErrorResume(error -> {
            System.err.println("Error executing " + this.getClass().getSimpleName() + " logic: " + error.getMessage());
            return Mono.just(NodeResult.failure(context, error, String.class)); // Pass payloadType
        });
    }
}
