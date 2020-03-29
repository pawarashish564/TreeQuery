package org.treequery.utils;

import org.treequery.util.JsonInstructionHelper;

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
}
