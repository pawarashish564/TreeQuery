package io.exp.treequery.beam.Transform;

import com.google.common.collect.Lists;
import io.exp.treequery.model.Node;
import lombok.Getter;

import java.util.List;
@Getter
public class JoinNode extends Node implements io.exp.treequery.model.JoinAble {
    List<Key> keys = Lists.newLinkedList();

    @Override
    public String execute() {
        return null;
    }

    @Override
    public void undo(String id) {

    }

}
