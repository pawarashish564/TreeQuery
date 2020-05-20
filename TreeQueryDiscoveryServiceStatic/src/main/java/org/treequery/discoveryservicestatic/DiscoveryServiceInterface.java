package org.treequery.discoveryservicestatic;

import org.treequery.cluster.Cluster;
import org.treequery.discoveryservicestatic.model.Location;


public interface DiscoveryServiceInterface {
    public void registerCacheResult(String identifier, Cluster clusterName);
    public Cluster getCacheResultCluster(String identifier);

    public void registerCluster(Cluster cluster, String address, int port);
    public Location getClusterLocation(Cluster cluster);
}
