package org.treequery.dto;

import lombok.Builder;
import lombok.Getter;
import org.treequery.model.Node;

import java.io.Serializable;

@Builder
@Getter
public class TreeNodeEvent implements Serializable {
    protected final String id;
    protected final Node calcNode;
    protected final Node notifyNode;
    protected final Node rootNode;
}
