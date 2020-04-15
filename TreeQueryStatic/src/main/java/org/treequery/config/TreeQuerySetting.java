package org.treequery.config;

import lombok.*;
import org.treequery.cluster.Cluster;

@ToString
@Getter
public class TreeQuerySetting {
    Cluster cluster;
    String servicehostname;
    int servicePort;
    String cacheFilePath;
    String redisHostName;
    int redisPort;

    public TreeQuerySetting(String cluster, String servicehostname, int servicePort,
                            String cacheFilePath, String redisHostName, int redisPort) {
        this.cluster = Cluster.builder().clusterName(cluster).build();
        this.servicehostname = servicehostname;
        this.servicePort = servicePort;
        this.cacheFilePath = cacheFilePath;
        this.redisHostName = redisHostName;
        this.redisPort = redisPort;
    }

    public static TreeQuerySettingBuilder builder() {
        return new TreeQuerySettingBuilder();
    }


    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class TreeQuerySettingBuilder {
        private String cluster;
        private String servicehostname;
        private int servicePort;
        @Setter
        private String cacheFilePath;
        private String redisHostName;
        private int redisPort;

        public TreeQuerySettingBuilder cluster(String cluster) {
            this.cluster = cluster;
            return this;
        }


        public TreeQuerySetting build() {
            return new TreeQuerySetting(cluster, servicehostname, servicePort, cacheFilePath, redisHostName, redisPort);
        }

        public String toString() {
            return "TreeQuerySetting.TreeQuerySettingBuilder(cluster=" + this.cluster + ", servicehostname=" + this.servicehostname + ", servicePort=" + this.servicePort + ", cacheFilePath=" + this.cacheFilePath + ", redisHostName=" + this.redisHostName + ", redisPort=" + this.redisPort + ")";
        }
    }
}
