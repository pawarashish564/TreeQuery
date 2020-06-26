package org.treequery.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.treequery.cluster.Cluster;
import org.treequery.model.CacheTypeEnum;

@Getter
public class TreeQuerySetting {
    private final Cluster cluster;
    private final String servicehostname;
    private final int servicePort;
    private final String cacheFilePath;
    private final String redisHostName;
    private final int redisPort;
    private final String serviceDiscoveryHostName;
    private final int serviceDiscoveryPort;
    private final CacheTypeEnum cacheTypeEnum = CacheTypeEnum.FILE;

    private TreeQuerySetting(String cluster, String servicehostname, int servicePort,
                            String cacheFilePath, String redisHostName, int redisPort,
                             String serviceDiscoveryHostName, int serviceDiscoveryPort) {
        this.cluster = Cluster.builder().clusterName(cluster).build();
        this.servicehostname = servicehostname;
        this.servicePort = servicePort;
        this.cacheFilePath = cacheFilePath;
        this.redisHostName = redisHostName;
        this.redisPort = redisPort;
        this.serviceDiscoveryHostName = serviceDiscoveryHostName;
        this.serviceDiscoveryPort = serviceDiscoveryPort;
    }

    public static TreeQuerySettingBuilder builder() {
        return new TreeQuerySettingBuilder();
    }

    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
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
        private  String serviceDiscoveryHostName;
        private  int serviceDiscoveryPort;

        public TreeQuerySettingBuilder cluster(String cluster) {
            this.cluster = cluster;
            return this;
        }


        public TreeQuerySetting build() {
            return new TreeQuerySetting(
                    cluster, servicehostname, servicePort,
                    cacheFilePath, redisHostName, redisPort,
                    serviceDiscoveryHostName, serviceDiscoveryPort);
        }

        public String toString() {
            return "TreeQuerySetting.TreeQuerySettingBuilder(cluster=" + this.cluster + ", servicehostname=" + this.servicehostname + ", servicePort=" + this.servicePort + ", cacheFilePath=" + this.cacheFilePath + ", redisHostName=" + this.redisHostName + ", redisPort=" + this.redisPort + ")";
        }
    }
}
