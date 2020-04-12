package org.treequery.utils;

import org.treequery.config.TreeQuerySetting;

import java.nio.file.Files;
import java.nio.file.Path;

public class SettingInitializer {
    public static TreeQuerySetting createTreeQuerySetting(){
        TreeQuerySetting.TreeQuerySettingBuilder treeQuerySettingBuilder = TreeQuerySetting.builder();
        treeQuerySettingBuilder.servicehostname("localhost");
        treeQuerySettingBuilder.servicePort(9002);
        try {
            Path path = Files.createTempDirectory("TreeQuery_");
            treeQuerySettingBuilder.cacheFilePath(path.toAbsolutePath().toString());
        }catch(Exception ex){}
        return treeQuerySettingBuilder.build();
    }
}
