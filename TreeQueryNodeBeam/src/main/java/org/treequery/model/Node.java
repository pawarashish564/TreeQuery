package org.treequery.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import org.treequery.cluster.Cluster;
import lombok.Getter;
import lombok.NonNull;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.common.collect.Lists;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;


@Getter
public abstract class Node implements Serializable {
    @NonNull
    protected String description;
    @NonNull
    protected ActionTypeEnum action;
    @NonNull
    protected Cluster cluster;

    private static final boolean SIMPLE_TOSTRING = true;

    List<Node> children = Lists.newLinkedList();

    public void insertChildren(Node childNode){
        this.children.add(childNode);
    }

    public boolean isSameCluster(Node node){
        return this.cluster.equals(node.cluster);
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setAction(ActionTypeEnum action) {
        this.action = action;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public void setBasicValue(JsonNode jsonNode){
        this.setDescription(jsonNode.get("description").asText());
        this.setAction(ActionTypeEnum.valueOf(jsonNode.get("action").asText()));
        this.setCluster(Cluster.builder()
                .clusterName(jsonNode.get("cluster").asText())
                .build());
    }

    public String toString() {
        if (SIMPLE_TOSTRING){
            return this.getDescription();
        }else {
            Gson gson = new Gson();
            return gson.toJson(this);
        }
    }

    public String getIdentifier(){
        return this.getSHA256();
    }
    private String getSHA256(){
        String sha256hex = Hashing.sha256()
                .hashString(this.toString(), StandardCharsets.UTF_8)
                .toString();
        return sha256hex;
    }

}
