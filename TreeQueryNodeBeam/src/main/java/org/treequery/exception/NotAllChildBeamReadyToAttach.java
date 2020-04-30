package org.treequery.exception;

import org.treequery.model.Node;

public class NotAllChildBeamReadyToAttach extends RuntimeException {
    public NotAllChildBeamReadyToAttach(Node node){
        super(String.format("Not all childen of Node %s for beam attachment ", node.getName()));
    }
}
