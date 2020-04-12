package org.treequery.config;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class TreeQuerySetting {
    String cluster;
    String servicehostname;
    int servicePort;
    String cacheFilePath;
    String redisHostName;
    int redisPort;
}
