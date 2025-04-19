package xyz.vvrf.reactor.dag.example.dataProcessingDag;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import xyz.vvrf.reactor.dag.core.DagNode;
import xyz.vvrf.reactor.dag.example.dataProcessingDag.node.*;

import java.util.List;

/**
 * DAG 相关的配置类，用于管理 DAG 及其节点的 Bean 定义
 */
@Configuration
public class DagConfiguration {

    /**
     * 定义 DAG 处理流程
     */
    @Bean
    public DataProcessingDag dataProcessingDag(List<DagNode<ProcessingContext, ?>> nodes) {
        return new DataProcessingDag(nodes);
    }

    /**
     * 注册数据获取节点
     */
    @Bean
    public DataFetchNode dataFetchNode() {
        return new DataFetchNode();
    }

    /**
     * 注册数据验证节点
     */
    @Bean
    public DataValidationNode dataValidationNode() {
        return new DataValidationNode();
    }

    /**
     * 注册数据转换节点
     */
    @Bean
    public DataTransformationNode dataTransformationNode() {
        return new DataTransformationNode();
    }

    /**
     * 注册数据富集节点
     */
    @Bean
    public DataEnrichmentNode dataEnrichmentNode() {
        return new DataEnrichmentNode();
    }

    /**
     * 注册结果组装节点
     */
    @Bean
    public ResultAssemblyNode resultAssemblyNode() {
        return new ResultAssemblyNode();
    }
}