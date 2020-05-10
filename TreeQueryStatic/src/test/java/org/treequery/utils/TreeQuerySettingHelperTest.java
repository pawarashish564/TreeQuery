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
        assertEquals(Cluster.builder().clusterName("A").build(), treeQuerySetting.getCluster());
        assertEquals(9002, treeQuerySetting.getServicePort());
        String tmpPath = treeQuerySetting.getCacheFilePath();
        assertTrue(Files.exists(Paths.get(tmpPath)));
    }
}

/*
package org.treequery.utils;

import org.junit.jupiter.api.Test;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;

import static org.junit.jupiter.api.Assertions.*;

class TreeQuerySettingHelperTest {

    @Test
    void createFromYaml() {
        TreeQuerySetting treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        assertEquals(Cluster.builder().clusterName("A").build(), treeQuerySetting.getCluster());
    }
}
 */