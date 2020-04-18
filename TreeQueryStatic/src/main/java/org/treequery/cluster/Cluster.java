package org.treequery.cluster;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Builder
@EqualsAndHashCode
@ToString
public class Cluster implements Serializable {
    @Getter
    private String clusterName;


}
