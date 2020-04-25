package org.treequery.service.proxy;

import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.NonNull;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.model.Location;
import org.treequery.grpc.client.TreeQueryClient;
import org.treequery.grpc.exception.NoLocationFoundForClusterException;
import org.treequery.grpc.model.TreeQueryResult;
import org.treequery.model.Node;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.StatusTreeQueryCluster;
import org.treequery.service.TreeQueryClusterRunner;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class GrpcTreeQueryClusterRunnerProxy implements TreeQueryClusterRunnerProxyInterface {
    private final TreeQueryRequest.RunMode runMode;
    @NonNull
    private final DiscoveryServiceInterface discoveryServiceInterface;
    private final boolean renewCache;
    private final Map<Cluster, TreeQueryClient> treeQueryClientMap = Maps.newConcurrentMap();

    @Builder
    GrpcTreeQueryClusterRunnerProxy(TreeQueryRequest.RunMode runMode,
                                    DiscoveryServiceInterface discoveryServiceInterface,
                                    boolean renewCache){
        this.runMode = runMode;
        this.discoveryServiceInterface = discoveryServiceInterface;
        this.renewCache = renewCache;
    }

    @Override
    public void runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> StatusCallback) {
        final Cluster cluster = node.getCluster();
        if (treeQueryClientMap.get(cluster) == null) {
            synchronized (treeQueryClientMap) {
                if (treeQueryClientMap.get(cluster) == null) {
                    Location location = Optional.ofNullable(discoveryServiceInterface.getClusterLocation(cluster))
                            .orElseThrow(() -> new NoLocationFoundForClusterException(cluster));
                    TreeQueryClient treeQueryClient = new TreeQueryClient(location.getAddress(), location.getPort());
                    treeQueryClientMap.put(cluster, treeQueryClient);
                }
            }
        }
        TreeQueryClient treeQueryClient = treeQueryClientMap.get(cluster);
        TreeQueryResult treeQueryResult = treeQueryClient.query(runMode, node.toJson(),renewCache,1,1 );
        StatusTreeQueryCluster.QueryTypeEnum queryTypeEnum;
        if (!treeQueryResult.getHeader().isSuccess()){
            queryTypeEnum = StatusTreeQueryCluster.QueryTypeEnum.FAIL;
        }else{
            queryTypeEnum = StatusTreeQueryCluster.QueryTypeEnum.SUCCESS;
        }
        StatusCallback.accept(
                StatusTreeQueryCluster.builder()
                        .status(queryTypeEnum)
                        .description(String.format("%d:%s",
                                treeQueryResult.getHeader().getErr_code(),
                                treeQueryResult.getHeader().getErr_msg()))
                        .node(node)
                        .cluster(cluster)
                .build()
        );
    }

}
