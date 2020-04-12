package org.treequery.config;

import lombok.Builder;
import org.treequery.cluster.Cluster;

@Builder
public class TreeQuerySetting {
    String cluster;
    String servicehostname;
    int servicePort;
    String cacheFilePath;
    String redisHostName;
    int redisPort;
}
