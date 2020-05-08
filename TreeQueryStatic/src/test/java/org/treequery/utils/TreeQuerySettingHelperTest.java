package org.treequery.utils;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;
@Slf4j
class TreeQuerySettingHelperTest {

    @Test
    void testCreateFromYaml() {
        TreeQuerySetting treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        assertEquals(Cluster.builder().clusterName("A").address("localhost").port(9002).build(), treeQuerySetting.getCluster());
        String tmpPath = treeQuerySetting.getCacheFilePath();
        assertTrue(Files.exists(Paths.get(tmpPath)));
    }
}