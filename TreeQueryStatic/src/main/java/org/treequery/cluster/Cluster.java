package org.treequery.cluster;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;

@Builder
@EqualsAndHashCode
@ToString
public class Cluster implements Serializable {
    private String clusterName;

}
