package org.treequery.discoveryservice.proxy;

import com.google.common.collect.Maps;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.model.Location;

import java.util.Map;

public class LocalDummyDiscoveryServiceProxy implements DiscoveryServiceInterface {
    Map<String, Cluster> cacheResultMap = Maps.newConcurrentMap();
    Map<Cluster, Location> clusterLocationMap = Maps.newConcurrentMap();
    @Override
    public void registerCacheResult(String hashId, Cluster cluster) {
        cacheResultMap.put(hashId, cluster);
    }

    @Override
    public Cluster getCacheResultCluster(String hashId) {
        return cacheResultMap.get(hashId);
    }

    @Override
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
