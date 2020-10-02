package org.treequery.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.treequery.cluster.ClusterDependencyGraph;
import org.treequery.model.Node;
import org.treequery.service.StatusTreeQueryCluster;

import java.io.Serializable;
import java.util.function.Consumer;

@Builder
@Getter
public class TreeNodeEvent implements Serializable {
    protected final String id;
    protected final Node calcNode;
    protected final Node rootNode;

    @NonNull protected final Consumer<StatusTreeQueryCluster> StatusCallback;
    @NonNull protected final ClusterDependencyGraph clusterDependencyGraph;
}
