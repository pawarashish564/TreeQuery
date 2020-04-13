package org.treequery.service;

import lombok.*;
import org.treequery.cluster.Cluster;
import org.treequery.model.Node;

@Getter
@Builder
@ToString
public class StatusTreeQueryCluster {
    @RequiredArgsConstructor
    public enum QueryTypeEnum {
        SUCCESS(0), FAIL(2), RUNNING(1), SYSTEMERROR(500);
        @Getter
        private final int value;
    }
    @NonNull
    QueryTypeEnum status;
    @NonNull
    String description;
    @NonNull
    Node node;
    @NonNull
    Cluster cluster;
}
