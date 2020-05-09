package org.treequery.exception;

import org.treequery.model.Node;

public class JoinOnlySupport2NodesException extends RuntimeException {
    public JoinOnlySupport2NodesException(Node node){
        super (String.format("Join Not support more than 2 data stream: %s", node.getName()));
    }
}
