package org.treequery.grpc.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.model.Location;
import org.treequery.grpc.exception.NoLocationFoundForClusterException;
import org.treequery.model.Node;
import org.treequery.utils.JsonInstructionHelper;

import java.util.Optional;

public class TreeQueryClientFactory {
    public static TreeQueryClient createTreeQueryClientFromJsonInput(String jsonInput,
                                                                     DiscoveryServiceInterface discoveryServiceInterface){
        Location location = null;
        TreeQueryClient treeQueryClient = null;
        try {
            Node rootNode = JsonInstructionHelper.createNode(jsonInput);
            final Cluster cluster = rootNode.getCluster();
            location = Optional.ofNullable(
                    discoveryServiceInterface
                            .getClusterLocation(cluster))
                    .orElseThrow(()->{
                        throw new NoLocationFoundForClusterException(cluster);
                    });
        }
        catch(JsonProcessingException je){
            throw new IllegalArgumentException(String.format("Not able to parse:%s - %s", jsonInput, je.getMessage()));
        }

        return new TreeQueryClient(location.getAddress(),
                location.getPort());
    }
}
