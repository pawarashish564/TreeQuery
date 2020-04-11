package org.treequery.discoveryservice.proxy;

import com.google.common.collect.Maps;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.model.Location;

import java.util.Map;

public class LocalDummyDiscoveryServiceProxy implements DiscoveryServiceInterface {
    Map<String, String> cacheResultMap = Maps.newConcurrentMap();
    Map<String, Location> clusterLocationMap = Maps.newConcurrentMap();
    @Override
    public void registerCacheResult(String hashId, String clusterName) {
        cacheResultMap.put(hashId, clusterName);
    }

    @Override
    public String getCacheResultCluster(String hashId) {
        return cacheResultMap.get(hashId);
    }

    @Override
    public void registerCluster(String clusterName, String address, int port) {
        clusterLocationMap.put(
                clusterName,
                new Location(address, port)
                );
    }

    @Override
    public Location getClusterLocation(String clusterName) {
        return clusterLocationMap.get(clusterName);
    }
}
