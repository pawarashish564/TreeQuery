package org.treequery.discoveryservice;

import org.treequery.discoveryservice.model.Location;

public interface DiscoveryServiceInterface {
    public void registerCacheResult(String hashId, String clusterName);
    public String getCacheResultCluster(String hashId);

    public void registerCluster(String clusterName, String address, int port);
    public Location getClusterLocation(String clusterName);
}
