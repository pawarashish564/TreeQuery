package org.treequery.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.treequery.config.TreeQuerySetting;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TreeQuerySettingHelper {
    static AtomicInteger atomicCounter = new AtomicInteger(0);

    public static TreeQuerySetting createFromYaml() {
        atomicCounter.incrementAndGet();
        TreeQuerySetting.TreeQuerySettingBuilder treeQuerySettingBuilder;

        // Loading the YAML file from the /resources folder
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("treeQuery.yaml").getFile());

        // Instantiating a new ObjectMapper as a YAMLFactory
        ObjectMapper om = new ObjectMapper(new YAMLFactory());

        // Mapping the values from the YAML file to the TreeQuerySetting class
        try {
            treeQuerySettingBuilder = om.readValue(file, TreeQuerySetting.TreeQuerySettingBuilder.class);
            prepareTempDirectory(treeQuerySettingBuilder);
        } catch (Exception ex) {
            log.error("Cannot read YAML file", ex);
            throw new IllegalArgumentException(String.format("Cannot read YAML file:%s", ex.getMessage()));
        }
        return treeQuerySettingBuilder.build();
    }

    private static void prepareTempDirectory(TreeQuerySetting.TreeQuerySettingBuilder treeQuerySettingBuilder){
        try {
            String _pathName = treeQuerySettingBuilder.getCacheFilePath().toUpperCase();
            if (_pathName.equals("$TMPDIR") || _pathName.equals("${TMPDIR}")){
                Path path = Files.createTempDirectory("TreeQuery_");
                log.info(String.format("Write cache File to path: %s", path.toAbsolutePath().toString()));
                treeQuerySettingBuilder.setCacheFilePath(path.toAbsolutePath().toString());
            }
        }catch(Exception ex){}
    }
}
