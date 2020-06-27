package org.treequery.grpc.hostutils;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.utils.TreeQuerySettingHelper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class HostInfoTest {

    @Test
    void shouldReturnIpList() {
        List<String> ipAddresslst = HostInfo.getMyAddress();
        assertThat(ipAddresslst).hasSizeGreaterThan(0);
        ipAddresslst.forEach(
                (ip)->{
                    log.debug(ip);
                }
        );
    }
    @Test
    void shouldInitialize(){
        String treeQueryYaml = "clusterRun.yaml";
        TreeQuerySetting treeQuerySetting = TreeQuerySettingHelper.createFromYaml(treeQueryYaml, false);
        assertThat(treeQuerySetting).isNotNull();
        assertThat(treeQuerySetting.getServicehostname()).isNull();
        assertThat(treeQuerySetting.getCluster()).isEqualTo(Cluster.builder().clusterName("A").build());
        assertThat(treeQuerySetting.getServiceDiscoveryPort()).isEqualTo(8082);
    }
}