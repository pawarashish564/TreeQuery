package org.treequery.service.proxy;

import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.NonNull;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.service.*;
import org.treequery.utils.AvroSchemaHelper;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;


public class LocalDummyTreeQueryClusterRunnerProxy implements TreeQueryClusterRunnerProxyInterface {


    Map<Cluster, TreeQueryClusterRunner> treeQueryClusterRunnerMap = Maps.newHashMap();
    @NonNull
    private final TreeQuerySetting treeQuerySetting;
    @NonNull
    private final AvroSchemaHelper avroSchemaHelper;
    @NonNull
    private Function<Cluster, TreeQueryClusterRunner> createLocalTreeQueryClusterRunnerFunc;

    @Builder
    LocalDummyTreeQueryClusterRunnerProxy(TreeQuerySetting treeQuerySetting, AvroSchemaHelper avroSchemaHelper, Function<Cluster, TreeQueryClusterRunner> createLocalTreeQueryClusterRunnerFunc){
        this.treeQuerySetting = treeQuerySetting;
        this.avroSchemaHelper = avroSchemaHelper;
        this.createLocalTreeQueryClusterRunnerFunc = createLocalTreeQueryClusterRunnerFunc;

    }

    @Override
    public void runQueryTreeNetwork(Node rootNode, Consumer<StatusTreeQueryCluster> statusCallback) {
        final Cluster cluster = rootNode.getCluster();

        if (treeQueryClusterRunnerMap.get(cluster) == null){
            TreeQueryClusterRunner treeQueryClusterRunner = createLocalTreeQueryClusterRunnerFunc.apply(cluster);
            treeQueryClusterRunner.setTreeQueryClusterRunnerProxyInterface(this);
            treeQueryClusterRunnerMap.put(cluster, treeQueryClusterRunner);
        }

        TreeQueryClusterRunner treeQueryClusterRunner = treeQueryClusterRunnerMap.get(cluster);

        treeQueryClusterRunner.runQueryTreeNetwork(rootNode, statusCallback);
    }

    @Override
    public void setTreeQueryClusterRunnerProxyInterface(TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface) {
        throw new NoSuchMethodError("A proxy not need a proxy interface");
    }
}
