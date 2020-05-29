package org.treequery.utils;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
@Slf4j
class DatabaseSettingHelperTest {
    static String databaseConnectionConfig;
    static String databaseConnectionFile;

    DatabaseSettingHelper databaseSettingHelper = null;

    @BeforeEach
    void  init(){
    }

    @Test
    void checkMongoConnectionString() {
        databaseConnectionConfig = "DatabaseConnection2.yaml";
        databaseSettingHelper =  DatabaseSettingHelper.initDatabaseSettingHelper(databaseConnectionConfig, false, true);
        // Loading the YAML file from the /resources folder
        String mongoString = databaseSettingHelper.getMongoConnectionString();
        log.debug(mongoString);
        assertNotNull(mongoString);
        assertEquals("mongodb://mongoadmin:secret@localhost:27017", mongoString);
    }
    @Test
    void checkMySqlConnectionString(){
        databaseConnectionConfig = "DatabaseConnection2.yaml";
        databaseSettingHelper =  DatabaseSettingHelper.initDatabaseSettingHelper(databaseConnectionConfig, false, true);
        String jdbcConnString = databaseSettingHelper.getJDBCConnectionString();
        assertEquals("jdbc:mysql://localhost:3306/ppmtcourse", jdbcConnString);
        String jdbcDriver = databaseSettingHelper.getJDBCDriver();
        assertEquals( "com.mysql.jdbc.Driver", jdbcDriver);
        String jdbcUser = databaseSettingHelper.getJDBCUser();
        assertEquals("root", jdbcUser);
        String jdbcPwd = databaseSettingHelper.getJDBCPassword();
        assertEquals("example", jdbcPwd);
    }
    @Test
    void checkMySqlConnectionString_injectedbyEnvVar() {
        databaseConnectionConfig = "DatabaseConnection.yaml";
        databaseSettingHelper =  DatabaseSettingHelper.initDatabaseSettingHelper(databaseConnectionConfig, false, true);
        String jdbcConnString = databaseSettingHelper.getJDBCConnectionString();
        assertEquals("jdbc:mysql://localhost:3306/ppmtcourse", jdbcConnString);
        String jdbcDriver = databaseSettingHelper.getJDBCDriver();
        assertEquals( "com.mysql.jdbc.Driver", jdbcDriver);
        String jdbcUser = databaseSettingHelper.getJDBCUser();
        assertEquals("root", jdbcUser);
        String jdbcPwd = databaseSettingHelper.getJDBCPassword();
        assertEquals("example", jdbcPwd);
    }
    @Test
    void checkMongoConnectionString_injectedbyEnvVar() {
        databaseConnectionConfig = "DatabaseConnection.yaml";
        databaseSettingHelper =  DatabaseSettingHelper.initDatabaseSettingHelper(databaseConnectionConfig, false, true);
        // Loading the YAML file from the /resources folder
        String mongoString = databaseSettingHelper.getMongoConnectionString();
        log.debug(mongoString);
        assertNotNull(mongoString);
        assertEquals("mongodb://mongoadmin:secret@localhost:27017", mongoString);
    }
    @Test
    void shouldGetSameInstanceAfterInit() {
        databaseConnectionConfig = "DatabaseConnection.yaml";
        DatabaseSettingHelper orgdatabaseSettingHelper =  DatabaseSettingHelper.initDatabaseSettingHelper(databaseConnectionConfig, false, true);
        DatabaseSettingHelper databaseSettingHelper = DatabaseSettingHelper.getDatabaseSettingHelper();
        assertEquals(orgdatabaseSettingHelper, databaseSettingHelper);
        databaseConnectionConfig = "DatabaseConnection2.yaml";
        databaseSettingHelper = DatabaseSettingHelper.initDatabaseSettingHelper(databaseConnectionConfig, false, false);
        assertEquals(orgdatabaseSettingHelper, databaseSettingHelper);
        databaseSettingHelper = DatabaseSettingHelper.initDatabaseSettingHelper(databaseConnectionConfig, false, true);
        assertNotEquals(orgdatabaseSettingHelper, databaseSettingHelper);

    }

}