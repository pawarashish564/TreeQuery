package org.treequery.utils;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
@Slf4j
class TreeQuerySettingHelperTest {

    @BeforeAll
    static void setEnvVarialble() throws Exception{
        Map<String, String> envMap = Maps.newHashMap();
        envMap.put("NODE_HOSTNAME", "localhost");
        envMap.put("NODE_PORT", "9000");
        envMap.put("CLUSTERNAME", "NodeA");
        envMap.put("SERVICE_DISCOVERY_HOSTNAME", "192.168.1.14");
        envMap.put("SERVICE_DISCOVERY_PORT", "9001");
        EnvVariableSetter.setEnv(envMap);
    }

    @Test
    void testCreateFromYaml() {
        TreeQuerySetting treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        assertEquals(Cluster.builder().clusterName("A").build(), treeQuerySetting.getCluster());
        assertEquals(9002, treeQuerySetting.getServicePort());
        String tmpPath = treeQuerySetting.getCacheFilePath();
        assertTrue(Files.exists(Paths.get(tmpPath)));
    }

    @Test
    void testyamltemplateReadEnvVariable() {
        String rawText = TreeQuerySettingHelper.readYamlFile("treequeryCluster.templ.yaml", false);
        String refText = TreeQuerySettingHelper.readYamlFile("treequeryCluster.sample.yaml", false);
        String yaml = TreeQuerySettingHelper.replaceEnvVariables(rawText);
        System.out.println(yaml);
        assertEquals(yaml, refText);
    }
    @Test
    void testSystemSettingFromYamlTemplate(){
        TreeQuerySetting treeQuerySetting = TreeQuerySettingHelper.createFromYaml("treequeryCluster.templ.yaml", false);

        assertAll(
                ()->{
                    assertEquals("localhost",treeQuerySetting.getServicehostname());
                },
                ()->{
                    assertEquals(9000,treeQuerySetting.getServicePort());
                },
                ()->{
                    assertEquals("NodeA", treeQuerySetting.getCluster().getClusterName());
                },
                ()->{
                    assertEquals("192.168.1.14", treeQuerySetting.getServiceDiscoveryHostName());
                },
                ()->{
                    assertEquals(9001, treeQuerySetting.getServiceDiscoveryPort());
                },
                ()->{
                    assertNotEquals("${TMPDIR}",treeQuerySetting.getCacheFilePath());
                }
        );


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