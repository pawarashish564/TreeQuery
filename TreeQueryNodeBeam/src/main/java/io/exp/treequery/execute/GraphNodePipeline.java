package io.exp.treequery.execute;

import com.google.common.collect.Maps;
import io.exp.treequery.cluster.Cluster;
import io.exp.treequery.model.Node;

import java.util.List;
import java.util.Map;

public  class GraphNodePipeline implements NodePipeline {
    Cluster cluster;
    PipelineBuilderInterface pipelineBuilderInterface;
    Map<String, List> graph = Maps.newHashMap();
    Map<String, List> depends = Maps.newHashMap();

    GraphNodePipeline(Cluster cluster, PipelineBuilderInterface pipelineBuilderInterface, Map<String, List> graph, Map<String, List> depends) {
        this.cluster = cluster;
        this.pipelineBuilderInterface = pipelineBuilderInterface;
        this.graph = graph;
        this.depends = depends;
    }

    public static GraphNodePipelineBuilder builder() {
        return new GraphNodePipelineBuilder();
    }


    @Override
    public NodePipeline addNodeToPipeline(Node parentNode, Node node) {
        return null;
    }


    public static class GraphNodePipelineBuilder {
        private Cluster cluster;
        private PipelineBuilderInterface pipelineBuilderInterface;
        private Map<String, List> graph= Maps.newHashMap();
        private Map<String, List> depends= Maps.newHashMap();

        GraphNodePipelineBuilder() {
        }

        public GraphNodePipelineBuilder cluster(Cluster cluster) {
            this.cluster = cluster;
            return this;
        }

        public GraphNodePipelineBuilder pipelineBuilderInterface(PipelineBuilderInterface pipelineBuilderInterface) {
            this.pipelineBuilderInterface = pipelineBuilderInterface;
            return this;
        }

        public GraphNodePipeline build() {
            return new GraphNodePipeline(cluster, pipelineBuilderInterface, graph, depends);
        }

        public String toString() {
            return "GraphNodePipeline.GraphNodePipelineBuilder(cluster=" + this.cluster + ", pipelineBuilderInterface=" + this.pipelineBuilderInterface + ", graph=" + this.graph + ", depends=" + this.depends + ")";
        }
    }
}
