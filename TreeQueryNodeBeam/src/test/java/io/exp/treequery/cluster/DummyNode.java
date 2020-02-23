package io.exp.treequery.cluster;

import io.exp.treequery.model.ActionTypeEnum;
import io.exp.treequery.model.Node;
import lombok.Builder;

public class DummyNode extends Node {
    @Override
    public String execute() {
        return null;
    }

    @Override
    public void undo(String id) {
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
}
