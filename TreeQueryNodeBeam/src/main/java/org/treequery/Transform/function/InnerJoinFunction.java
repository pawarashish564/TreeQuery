package org.treequery.Transform.function;

import com.google.common.collect.Lists;
import lombok.Getter;
import org.treequery.model.JoinAble;

import java.util.List;
@Getter
public class InnerJoinFunction implements JoinAble {
    List<Key> keys = Lists.newLinkedList();

}
