package org.treequery.config;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class TreeQuerySetting {
    String cluster;
    String servicehostname;
    int servicePort;
    String cacheFilePath;
    String redisHostName;
    int redisPort;
}
