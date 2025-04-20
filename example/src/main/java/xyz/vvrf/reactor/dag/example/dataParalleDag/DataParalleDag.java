package xyz.vvrf.reactor.dag.example.dataParalleDag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired; // Import Autowired
import org.springframework.stereotype.Component;
import xyz.vvrf.reactor.dag.builder.ChainBuilder;
import xyz.vvrf.reactor.dag.core.DagNode;
// Removed unused ProcessingContext import
import xyz.vvrf.reactor.dag.core.DependencyDescriptor;
import xyz.vvrf.reactor.dag.example.dataParalleDag.node.*;
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * reactor-dag
 * Defines a DAG with a fan-out/fan-in structure for parallel execution simulation.
 *
 * @author ruifeng.wen
 * @date 4/19/25 (modified)
 */
@Component
@Slf4j
public class DataParalleDag extends AbstractDagDefinition<ParalleContext> {

    @Autowired
    public DataParalleDag(List<DagNode<ParalleContext, ?, ?>> nodes) {
        super(ParalleContext.class, nodes);

        log.info("开始为 DAG '{}' 配置显式依赖关系...", getDagName());
        // 3. 创建 ChainBuilder 实例
        ChainBuilder<ParalleContext> builder = new ChainBuilder<>(this);

        Map<String, List<DependencyDescriptor>> explicitDependencies = builder
                // 定义起点节点 (无依赖)
                .node(FirstNode.class) // 假设 SessionNode 已注册
                .node(ParallelNodeA.class,FirstNode.class)
                .node(ParallelNodeB.class,FirstNode.class)
                .node(ParallelNodeC.class,FirstNode.class)
                .node(FinalNode.class,ParallelNodeA.class,ParallelNodeB.class,ParallelNodeC.class)
                .build();
        explicitDependencies.forEach(this::addExplicitDependencies);
        initializeIfNeeded();
    }
}
