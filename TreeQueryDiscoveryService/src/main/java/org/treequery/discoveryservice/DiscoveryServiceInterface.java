package org.treequery.discoveryservice;

import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.model.Location;
import java.io.Serializable;

public interface DiscoveryServiceInterface extends Serializable {
    public void registerCacheResult(String identifier, Cluster clusterName);
    public Cluster getCacheResultCluster(String identifier);

    public void registerCluster(Cluster cluster, String address, int port);
    public Location getClusterLocation(Cluster cluster);
}
