package org.treequery.Transform;

import com.google.common.collect.Lists;
import org.treequery.Transform.function.InnerJoinAble;
import org.treequery.model.JoinAble;
import org.treequery.model.Node;
import lombok.Getter;

import java.util.List;

public class JoinNode extends Node  {

    public JoinNode(){
        this.joinFunction = new InnerJoinAble();
    }

}
