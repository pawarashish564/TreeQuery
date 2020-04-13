package org.treequery.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.treequery.config.TreeQuerySetting;

import java.io.File;

@Slf4j
public class TreeQuerySettingHelper {
    public static TreeQuerySetting createFromYaml() {
        TreeQuerySetting setting;

        // Loading the YAML file from the /resources folder
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("treeQuery.yaml").getFile());

        // Instantiating a new ObjectMapper as a YAMLFactory
        ObjectMapper om = new ObjectMapper(new YAMLFactory());

        // Mapping the values from the YAML file to the TreeQuerySetting class
        try {
            setting = om.readValue(file, TreeQuerySetting.class);
        } catch (Exception ex) {
            log.error("Cannot read YAML file", ex);
            setting = new TreeQuerySetting();
        }
        return setting;
    }
}
