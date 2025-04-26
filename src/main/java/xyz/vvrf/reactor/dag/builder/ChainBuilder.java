package xyz.vvrf.reactor.dag.builder;

import lombok.extern.slf4j.Slf4j;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.core.InputRequirement;
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DAG 链式构建器。
 * 用于以编程方式定义节点间的执行顺序依赖 (边) 和输入数据映射。
 *
 * @param <C> 上下文类型
 * @author ruifeng.wen (Refactored by Devin)
 */
@Slf4j
public class ChainBuilder<C> {

    private final AbstractDagDefinition<C> dagDefinition;
    // 存储执行依赖关系：目标节点 -> [源节点列表]
    private final Map<String, List<String>> executionDependencies = new ConcurrentHashMap<>();
    // 存储输入映射：目标节点 -> [输入需求 -> 源节点名称]
    private final Map<String, Map<InputRequirement<?>, String>> inputMappings = new ConcurrentHashMap<>();

    /**
     * 创建 ChainBuilder 实例。
     *
     * @param dagDefinition 正在配置的 DagDefinition 实例。
     *                      必须是 AbstractDagDefinition 的子类，且已完成节点注册。
     */
    public ChainBuilder(AbstractDagDefinition<C> dagDefinition) {
        this.dagDefinition = Objects.requireNonNull(dagDefinition, "DagDefinition 不能为空");
        log.info("[{}] DAG '{}': ChainBuilder 已创建。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());
    }

    /**
     * 定义一个节点的执行顺序依赖。
     * 指明 `targetNodeName` 必须在所有 `sourceNodeNames` 执行完成后才能执行。
     * 如果一个节点被多次调用此方法，后续调用会覆盖之前的执行依赖设置。
     *
     * @param targetNodeName      目标节点的名称 (必须已在 DagDefinition 注册)。
     * @param sourceNodeNames 它在执行顺序上依赖的节点的名称数组。如果为空或不传，表示此节点是执行起点之一。
     * @return 当前 ChainBuilder 实例，支持链式调用。
     * @throws IllegalArgumentException 如果目标节点或源节点未在 DagDefinition 中注册。
     * @throws NullPointerException 如果节点名称为 null。
     */
    public ChainBuilder<C> node(String targetNodeName, String... sourceNodeNames) {
        Objects.requireNonNull(targetNodeName, "目标节点名称不能为空");

        validateNodeExists(targetNodeName, "目标节点");

        List<String> sources = new ArrayList<>();
        if (sourceNodeNames != null) {
            for (String sourceNodeName : sourceNodeNames) {
                Objects.requireNonNull(sourceNodeName, "源节点名称不能为空");
                validateNodeExists(sourceNodeName, "源节点");
                sources.add(sourceNodeName);
            }
        }

        log.debug("[{}] DAG '{}': Builder 定义节点 '{}' 执行依赖于: {}",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                targetNodeName, sources.isEmpty() ? "<无>" : sources);
        executionDependencies.put(targetNodeName, sources);
        return this;
    }

    /**
     * 定义一个输入数据映射 (无限定符)。
     * 指明 `targetNodeName` 需要的类型为 `inputType` 的输入，由 `sourceNodeName` 提供。
     *
     * @param <T>            输入类型
     * @param targetNodeName 目标节点名称
     * @param inputType      目标节点所需的输入 Payload 类型
     * @param sourceNodeName 提供该输入的源节点名称
     * @return 当前 ChainBuilder 实例。
     * @throws IllegalArgumentException 如果节点未注册，或源节点不产生兼容的类型。
     */
    public <T> ChainBuilder<C> mapInput(String targetNodeName, Class<T> inputType, String sourceNodeName) {
        return mapInputInternal(targetNodeName, InputRequirement.require(inputType), sourceNodeName);
    }

    /**
     * 定义一个输入数据映射 (带限定符)。
     * 指明 `targetNodeName` 需要的类型为 `inputType` 且限定符为 `qualifier` 的输入，由 `sourceNodeName` 提供。
     *
     * @param <T>            输入类型
     * @param targetNodeName 目标节点名称
     * @param inputType      目标节点所需的输入 Payload 类型
     * @param qualifier      输入限定符
     * @param sourceNodeName 提供该输入的源节点名称
     * @return 当前 ChainBuilder 实例。
     * @throws IllegalArgumentException 如果节点未注册，或源节点不产生兼容的类型。
     */
    public <T> ChainBuilder<C> mapInput(String targetNodeName, Class<T> inputType, String qualifier, String sourceNodeName) {
        Objects.requireNonNull(qualifier, "Qualifier cannot be null when mapping input with qualifier");
        return mapInputInternal(targetNodeName, InputRequirement.require(inputType, qualifier), sourceNodeName);
    }

    /**
     * 定义一个输入数据映射 (使用 InputRequirement 对象)。
     *
     * @param <T>            输入类型
     * @param targetNodeName 目标节点名称
     * @param requirement    描述所需输入的 InputRequirement 对象 (其 optional 标志在此处被忽略，映射总是显式的)
     * @param sourceNodeName 提供该输入的源节点名称
     * @return 当前 ChainBuilder 实例。
     * @throws IllegalArgumentException 如果节点未注册，或源节点不产生兼容的类型。
     */
    public <T> ChainBuilder<C> mapInput(String targetNodeName, InputRequirement<T> requirement, String sourceNodeName) {
        Objects.requireNonNull(requirement, "InputRequirement cannot be null");
        // 重新创建一个非 optional 的 requirement 用于映射 key，因为映射本身是显式的
        InputRequirement<T> mappingKey = requirement.getQualifier()
                .map(q -> InputRequirement.require(requirement.getType(), q))
                .orElseGet(() -> InputRequirement.require(requirement.getType()));
        return mapInputInternal(targetNodeName, mappingKey, sourceNodeName);
    }

    // 内部映射逻辑
    private <T> ChainBuilder<C> mapInputInternal(String targetNodeName, InputRequirement<T> requirement, String sourceNodeName) {
        Objects.requireNonNull(targetNodeName, "目标节点名称不能为空");
        Objects.requireNonNull(sourceNodeName, "源节点名称不能为空");

        // 验证节点存在性
        validateNodeExists(targetNodeName, "目标节点");
        DagNode<C, ?, ?> sourceNode = validateNodeExists(sourceNodeName, "源节点");

        // 验证源节点是否能提供所需类型
        Class<?> sourceOutputType = sourceNode.getPayloadType();
        if (!requirement.getType().isAssignableFrom(sourceOutputType)) {
            String errorMsg = String.format("输入映射错误：源节点 '%s' (输出 %s) 不能满足目标节点 '%s' 对类型 '%s' 的输入需求。",
                    sourceNodeName, sourceOutputType.getSimpleName(), targetNodeName, requirement.getType().getSimpleName());
            log.error("[{}] {}", dagDefinition.getContextType().getSimpleName(), errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        log.debug("[{}] DAG '{}': Builder 定义输入映射: {} 的需求 {} 由 {} 提供",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(),
                targetNodeName, requirement, sourceNodeName);

        inputMappings.computeIfAbsent(targetNodeName, k -> new ConcurrentHashMap<>())
                .put(requirement, sourceNodeName);

        return this;
    }

    /**
     * 验证节点是否存在于 DagDefinition 中。
     * @param nodeName 节点名称
     * @param nodeRole 节点角色描述 (用于错误消息)
     * @return 找到的 DagNode 实例
     * @throws IllegalArgumentException 如果节点不存在
     */
    private DagNode<C, ?, ?> validateNodeExists(String nodeName, String nodeRole) {
        return dagDefinition.getNodeAnyType(nodeName)
                .orElseThrow(() -> {
                    String errorMsg = String.format("%s '%s' 未在 DagDefinition '%s' 中注册。",
                            nodeRole, nodeName, dagDefinition.getDagName());
                    log.error("[{}] {}", dagDefinition.getContextType().getSimpleName(), errorMsg);
                    return new IllegalArgumentException(errorMsg);
                });
    }


    /**
     * 将通过此 Builder 定义的所有配置应用到关联的 DagDefinition 实例。
     * 这包括执行依赖和输入映射。
     * 调用此方法后，通常应接着调用 {@link AbstractDagDefinition#initialize()}。
     *
     * @throws IllegalStateException 如果在应用配置时发生错误。
     */
    public void applyToDefinition() {
        log.info("[{}] DAG '{}': 开始将 ChainBuilder 定义的配置应用到 DagDefinition...",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());

        // 1. 应用执行依赖
        log.info("[{}] DAG '{}': 应用 {} 个节点的执行依赖...",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(), executionDependencies.size());
        dagDefinition.setExecutionDependencies(executionDependencies); // 假设 Definition 有此方法
        log.info("[{}] DAG '{}': 执行依赖应用完成。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());


        // 2. 应用输入映射
        log.info("[{}] DAG '{}': 应用 {} 个节点的输入映射...",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName(), inputMappings.size());
        dagDefinition.setInputMappings(inputMappings); // 假设 Definition 有此方法
        log.info("[{}] DAG '{}': 输入映射应用完成。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());

        log.info("[{}] DAG '{}': ChainBuilder 配置成功应用到 DagDefinition。",
                dagDefinition.getContextType().getSimpleName(), dagDefinition.getDagName());
    }

}
