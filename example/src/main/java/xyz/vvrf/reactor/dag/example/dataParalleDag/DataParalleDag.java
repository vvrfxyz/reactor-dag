package xyz.vvrf.reactor.dag.example.dataParalleDag;

import org.springframework.beans.factory.annotation.Autowired; // Import Autowired
import org.springframework.stereotype.Component;
import xyz.vvrf.reactor.dag.core.DagNode;
// Removed unused ProcessingContext import
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition;

import java.util.List;
import java.util.stream.Collectors;

/**
 * reactor-dag
 * Defines a DAG with a fan-out/fan-in structure for parallel execution simulation.
 *
 * @author ruifeng.wen
 * @date 4/19/25 (modified)
 */
@Component
public class DataParalleDag extends AbstractDagDefinition<ParalleContext> {

    @Autowired
    public DataParalleDag(List<DagNode<ParalleContext, ?, ?>> nodes) {
        super(ParalleContext.class, nodes);
        initializeIfNeeded();
    }

}
