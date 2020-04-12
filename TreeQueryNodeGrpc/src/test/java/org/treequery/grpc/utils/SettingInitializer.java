package org.treequery.grpc.utils;

import lombok.extern.slf4j.Slf4j;
import org.treequery.config.TreeQuerySetting;

import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class SettingInitializer {
    public static TreeQuerySetting createTreeQuerySetting(){
        TreeQuerySetting.TreeQuerySettingBuilder treeQuerySettingBuilder = TreeQuerySetting.builder();
        treeQuerySettingBuilder.servicehostname("localhost");
        treeQuerySettingBuilder.servicePort(9002);
        try {
            Path path = Files.createTempDirectory("TreeQuery_");
            log.info(String.format("Write cache File to path: %s", path.toAbsolutePath().toString()));
            treeQuerySettingBuilder.cacheFilePath(path.toAbsolutePath().toString());
        }catch(Exception ex){}
        return treeQuerySettingBuilder.build();
    }
}
