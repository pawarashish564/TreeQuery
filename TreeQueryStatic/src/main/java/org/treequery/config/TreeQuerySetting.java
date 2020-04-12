package org.treequery.config;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.treequery.cluster.Cluster;

@Builder
@Getter
@ToString
public class TreeQuerySetting {
    Cluster cluster;
    String servicehostname;
    int servicePort;
    String cacheFilePath;
    String redisHostName;
    int redisPort;
}
