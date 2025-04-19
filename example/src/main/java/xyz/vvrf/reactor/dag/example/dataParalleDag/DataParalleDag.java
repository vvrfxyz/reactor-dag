package xyz.vvrf.reactor.dag.example.dataParalleDag;

import org.springframework.stereotype.Component;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.example.dataProcessingDag.ProcessingContext;
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition;

import java.util.List;

/**
 * reactor-dag
 *
 * @author ruifeng.wen
 * @date 4/19/25
 */
@Component
public class DataParalleDag extends AbstractDagDefinition<ParalleContext> {
    public DataParalleDag(List<DagNode<ParalleContext, ?>> nodes) {
        super(ParalleContext.class, nodes);
        System.out.println("DataParalleDag initialized with nodes: " +
                nodes.stream().map(DagNode::getName).toList());
        initializeIfNeeded();
    }
}
