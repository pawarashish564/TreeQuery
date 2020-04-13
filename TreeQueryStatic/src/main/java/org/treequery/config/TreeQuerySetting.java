package org.treequery.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TreeQuerySetting {
    String cluster;
    String servicehostname;
    int servicePort;
    String cacheFilePath;
    String redisHostName;
    int redisPort;
}
