package org.treequery.Transform;

import com.google.common.collect.Lists;
import org.treequery.model.JoinAble;
import org.treequery.model.Node;
import lombok.Getter;

import java.util.List;
@Getter
public class JoinNode extends Node implements JoinAble {
    List<Key> keys = Lists.newLinkedList();



}
