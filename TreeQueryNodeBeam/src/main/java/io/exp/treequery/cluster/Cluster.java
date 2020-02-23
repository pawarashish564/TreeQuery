package io.exp.treequery.cluster;

import lombok.Builder;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Builder
@EqualsAndHashCode
public class Cluster implements Serializable {
    private String clusterName;
}
