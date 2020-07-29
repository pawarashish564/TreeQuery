package org.treequery.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.treequery.config.TreeQuerySetting;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class TreeQuerySettingHelper {
    static AtomicInteger atomicCounter = new AtomicInteger(0);
    static String envPattern = "(\\$\\{\\w+\\})|(\\$\\w+)";

    @SneakyThrows
    static String readYamlFile(String fileName, boolean realPath){
        File file = null;
        if(!realPath) {
            // Loading the YAML file from the /resources folder
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            file = new File(classLoader.getResource(fileName).getFile());
        }else{
            file = new File(fileName);
        }

        BufferedReader inputReader = new BufferedReader(new FileReader(file)) ;
        StringBuilder stringBuilder = new StringBuilder();
        String line = null;
        while ((line = inputReader.readLine()) !=null){
            stringBuilder.append(line).append(System.lineSeparator());
        }
        return stringBuilder.toString();
    }
    private static String convert(String token) {
        String envVariable;
        int startInx=1, length = token.length()-1;
        if (token.charAt(1) == '{'){
            startInx = 2;
            length -= 2;
        }
        envVariable = token.substring(startInx, length+startInx);
        String value = System.getenv(envVariable);

        return Optional.ofNullable(value).orElse(token);
    }
    protected static String replaceEnvVariables(String inputYaml){
        int lastIndex = 0;
        StringBuilder output = new StringBuilder();
        Pattern pattern =  Pattern.compile(envPattern);
        Matcher matcher = pattern.matcher(inputYaml);
        while (matcher.find()) {
            output.append(inputYaml, lastIndex, matcher.start())
                    .append(convert(matcher.group(1)));


            lastIndex = matcher.end();
        }
        if (lastIndex < inputYaml.length()) {
            output.append(inputYaml, lastIndex, inputYaml.length());
        }
        return output.toString();

    }

    public static TreeQuerySetting createFromYaml(){
        return TreeQuerySettingHelper.createFromYaml("treeQuery.yaml", false);
    }
    public static TreeQuerySetting createFromYaml(String fileName, boolean realPath) {
        atomicCounter.incrementAndGet();
        TreeQuerySetting setting;
        TreeQuerySetting.TreeQuerySettingBuilder treeQuerySettingBuilder = TreeQuerySetting.builder();

        String yamlFileContent = readYamlFile(fileName, realPath);
        String yaml = TreeQuerySettingHelper.replaceEnvVariables(yamlFileContent);
        // Instantiating a new ObjectMapper as a YAMLFactory
        ObjectMapper om = new ObjectMapper(new YAMLFactory());

        // Mapping the values from the YAML file to the TreeQuerySetting class
        try {
            treeQuerySettingBuilder = om.readValue(yaml, TreeQuerySetting.TreeQuerySettingBuilder.class);
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
                Path path = Files.createTempDirectory("TreeQuery"+treeQuerySettingBuilder.getCluster()+"_");
                log.info(String.format("File Cache write cache File to path: %s", path.toAbsolutePath().toString()));
                treeQuerySettingBuilder.setCacheFilePath(path.toAbsolutePath().toString());
            }
        }catch(Exception ex){}
    }
}
