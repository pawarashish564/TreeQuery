package org.treequery.Transform;

import org.treequery.Transform.function.JoinFunction;
import org.treequery.model.JoinAble;
import org.treequery.model.Node;

public class JoinNode extends Node  {
    public JoinNode(){
        this.joinFunction = null;
    }
    public JoinNode(JoinAble joinAble){
        this.joinFunction = joinAble;
    }
}
