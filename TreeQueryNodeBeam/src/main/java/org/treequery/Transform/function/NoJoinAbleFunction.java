package org.treequery.Transform.function;

import org.treequery.model.JoinAble;

import java.util.List;

public class NoJoinAbleFunction implements JoinAble {
    @Override
    public List<JoinKey> getJoinKeys() {
        throw new NoSuchMethodError("Join not supported");
    }
}
