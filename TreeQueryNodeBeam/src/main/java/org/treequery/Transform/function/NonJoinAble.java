package org.treequery.Transform.function;

import org.treequery.model.JoinAble;

import java.util.List;

public class NonJoinAble implements JoinAble {
    @Override
    public List<Key> getKeys() {
        throw new IllegalStateException("Join not supported");
    }
}
