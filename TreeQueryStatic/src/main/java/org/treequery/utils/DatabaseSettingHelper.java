package org.treequery.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.treequery.config.TreeQuerySetting;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class DatabaseSettingHelper {
    private final Map dbConfigMap;
    private final String realPathName;

    private static final String envRegex = "^\\$\\{(\\w+)\\}$";
    private static Pattern envEnvPattern;

    private static DatabaseSettingHelper databaseSettingHelper = null;

    public static DatabaseSettingHelper initDatabaseSettingHelper(String fileName, boolean realPath, boolean force){
        synchronized (DatabaseSettingHelper.class){
            if (databaseSettingHelper != null && !force){
                return databaseSettingHelper;
            }
            databaseSettingHelper = new DatabaseSettingHelper(fileName, realPath);
            return databaseSettingHelper;
        }
    }

    public static DatabaseSettingHelper getDatabaseSettingHelper(){
        synchronized (DatabaseSettingHelper.class){
            if (databaseSettingHelper == null){
                throw new IllegalStateException("DatabaseSettingHelper not yet initialized");
            }
            return databaseSettingHelper;
        }
    }

    private  DatabaseSettingHelper (String fileName, boolean realPath){
        if (!realPath){
            realPathName = getClassLoaderPath(fileName);
        }else{
            realPathName = fileName;
        }
        dbConfigMap = readDatabaseConnectionConfig(realPathName);
        envEnvPattern = Pattern.compile(envRegex);
    }

    private static String getEnvVariableValue(String value){
        Matcher matcher = envEnvPattern.matcher(value);
        if (!matcher.matches()){
            return value;
        }
        String envKey = matcher.group(1);
        String envVar = System.getenv(envKey);
        return Optional.ofNullable(envVar).orElseThrow(
                ()->new NullPointerException(
                        String.format("Failed to find %s", envKey)
                )
        );
    }

    private String getValue(String key) {
        String value = Optional.ofNullable(dbConfigMap.get(key)).orElseThrow(
                ()->new NullPointerException(
                        String.format("Not found %s in %s", key, realPathName)
                )
        ).toString();

        return getEnvVariableValue(value);
    }

    public String getMongoConnectionString(){
        String mongoUserName = getValue("mongo.UserName");
        String mongoPassword = getValue("mongo.Password");
        String hostName = getValue( "mongo.Hostname");
        String port = getValue("mongo.Port");
        return String.format(
                "mongodb://%s:%s@%s:%s",
                mongoUserName, mongoPassword,
                hostName, port
        );
    }

    public String getJDBCConnectionString(){
        String jdbcType = getValue("jdbc.Type");
        String jdbcHostname = getValue("jdbc.Hostname");
        String jdbcPort = getValue("jdbc.Port");
        String jdbcDatabase = getValue("jdbc.Database");

        String jdbcUserName = getValue("jdbc.UserName");
        String jdbcPassword = getValue("jdbc.Password");
        //"jdbc:mysql://localhost:3306/ppmtcourse"
        return String.format(
                "jdbc:%s://%s:%s/%s",
                jdbcType, jdbcHostname, jdbcPort,
                jdbcDatabase
        );
    }
    public String getJDBCDriver(){
        return getValue("jdbc.Driver");
    }
    public String getJDBCUser(){
        return getValue("jdbc.UserName");
    }
    public String getJDBCPassword(){
        return getValue("jdbc.Password");
    }

    static Map readDatabaseConnectionConfig(String fileName){
        Map dbConfigMap = null;
        // Mapping the values from the YAML file to the TreeQuerySetting class
        try {
            Yaml yaml = new Yaml();
            File file = new File(fileName);
            InputStream inputStream = new FileInputStream(file);
            dbConfigMap = yaml.load(inputStream);
        } catch (Exception ex) {
            log.error("Cannot read YAML file", ex);
            throw new IllegalArgumentException(String.format("Cannot read YAML file:%s", ex.getMessage()));
        }
        return dbConfigMap;
    }
    static String getClassLoaderPath(String fileName){
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());
        return file.getAbsolutePath();
    }


}
