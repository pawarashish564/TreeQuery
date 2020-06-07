package org.treequery.cluster;

import lombok.*;

import java.io.Serializable;

@Builder
@EqualsAndHashCode
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Cluster implements Serializable {
    @Getter
    private String clusterName;
}
