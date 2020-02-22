package io.exp.treequery.beam.Transform;

import io.exp.treequery.model.Node;
import lombok.Getter;


import java.util.List;
import java.util.Map;
@Getter
public class JoinNode extends Node implements io.exp.treequery.model.JoinAble {
    List<Map<Integer, String>> keys;

    @Override
    public String execute() {
        return null;
    }

    @Override
    public void undo(String id) {

    }
}
