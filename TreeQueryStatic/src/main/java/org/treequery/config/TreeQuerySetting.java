package org.treequery.config;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class TreeQuerySetting {
    String cluster;
    @Builder.Default
    String servicehostname = "localhost";
    @Builder.Default
    int servicePort = 9002;
    @Builder.Default
    String cacheFilePath = "./TreeQuery";
    String redisHostName;
    int redisPort;
}
