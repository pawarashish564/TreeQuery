package org.treequery.grpc.utils;

import org.treequery.config.TreeQuerySetting;
import org.treequery.utils.JsonInstructionHelper;

import java.io.File;

public class TestDataAgent {
    public static String prepareNodeFromJsonInstruction(String jsonFileName){
        String workDirectory = null;
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File jsonFile = new File(classLoader.getResource(jsonFileName).getFile());
        workDirectory = jsonFile.getParent();

        String jsonString = JsonInstructionHelper.parseJsonFile(jsonFile.getAbsolutePath());
        return jsonString.replaceAll("\\$\\{WORKDIR\\}", workDirectory);
    }
    public static String getTestResourceDirectory(String filename){
        String workDirectory = null;
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        File resourceFile = new File(classLoader.getResource(filename).getFile());
        return resourceFile.getParent();
    }

    public static TreeQuerySetting getTreeQuerySettingBackedByResources(String HOSTNAME, int PORT, String avroSampleFile){
        TreeQuerySetting.TreeQuerySettingBuilder treeQuerySettingBuilder = new TreeQuerySetting.TreeQuerySettingBuilder(
                "A",
                HOSTNAME,
                PORT,
                TestDataAgent.getTestResourceDirectory(avroSampleFile),
                "",0,"",0
        );
        return treeQuerySettingBuilder.build();
    }
}
