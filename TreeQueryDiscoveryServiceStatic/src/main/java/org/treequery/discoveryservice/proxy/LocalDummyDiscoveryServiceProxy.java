package org.treequery.discoveryservice.proxy;

import com.google.common.collect.Maps;
import lombok.ToString;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.model.Location;

import java.util.Map;

@ToString
public class LocalDummyDiscoveryServiceProxy implements DiscoveryServiceInterface {
    Map<String, Cluster> cacheResultMap = Maps.newHashMap();
    Map<Cluster, Location> clusterLocationMap = Maps.newHashMap();

    @Override
    public void registerCacheResult(String hashId, Cluster cluster) {

        synchronized (cacheResultMap) {
            cacheResultMap.put(hashId, cluster);
        }
    }

    @Override
    public Cluster getCacheResultCluster(String hashId) {
        synchronized (cacheResultMap) {
            return cacheResultMap.get(hashId);
        }
    }

    public void registerCluster(Cluster cluster, String address, int port) {
        clusterLocationMap.put(
                cluster,
                new Location(address, port)
        );
    }

    @Override
    public Location getClusterLocation(Cluster cluster) {
        return clusterLocationMap.get(cluster);
    }
}
