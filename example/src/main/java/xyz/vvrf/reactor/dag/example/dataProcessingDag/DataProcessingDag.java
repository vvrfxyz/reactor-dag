package xyz.vvrf.reactor.dag.example.dataProcessingDag;

/**
 * reactor-dag-example
 *
 * @author ruifeng.wen
 * @date 2025/4/2
 */

import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.impl.AbstractDagDefinition;

import java.util.List;

/**
 * 数据处理DAG定义
 */
public class DataProcessingDag extends AbstractDagDefinition<ProcessingContext> {
    public DataProcessingDag(List<DagNode<ProcessingContext, ?>> nodes) {
        super(ProcessingContext.class, nodes);
        initializeIfNeeded();
    }
}