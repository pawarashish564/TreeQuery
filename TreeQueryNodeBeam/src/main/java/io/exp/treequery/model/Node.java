package io.exp.treequery.model;

import com.google.gson.Gson;
import io.exp.treequery.cluster.Cluster;
import lombok.Getter;
import lombok.NonNull;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;


@Getter
public abstract class Node implements Command, Serializable {
    @NonNull
    protected String description;
    @NonNull
    protected ActionTypeEnum action;
    @NonNull
    protected Cluster cluster;

    List<Node> children = Lists.newLinkedList();

    public void insertChildren(Node childNode){
        this.children.add(childNode);
    }

    public boolean isSameCluster(Node node){
        return this.cluster.equals(node.cluster);
    }


    public String toString() {
        Gson gson  = new Gson();
        return gson.toJson(this);
        //return "Node(description=" + this.getDescription() + ", action=" + this.getAction() + ", cluster=" + this.getCluster() + ", children=" + this.getChildren() + ")";
    }
}
