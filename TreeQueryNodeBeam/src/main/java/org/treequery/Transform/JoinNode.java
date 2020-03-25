package org.treequery.Transform;

import org.treequery.Transform.function.InnerJoinFunction;
import org.treequery.model.JoinAble;
import org.treequery.model.Node;

public class JoinNode extends Node  {

    public JoinNode(){
        this.joinFunction = new InnerJoinFunction();
    }
    public JoinNode(JoinAble joinAble){
        this.joinFunction = joinAble;
    }
}
