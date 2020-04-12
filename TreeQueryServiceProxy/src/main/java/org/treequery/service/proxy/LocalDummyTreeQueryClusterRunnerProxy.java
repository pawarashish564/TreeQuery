package org.treequery.service.proxy;

import com.google.common.collect.Maps;
import lombok.Builder;
import org.apache.avro.generic.GenericRecord;
import org.treequery.beam.cache.BeamCacheOutputBuilder;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.model.CacheTypeEnum;
import org.treequery.model.Node;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.*;
import org.treequery.utils.AvroSchemaHelper;

import java.util.Map;
import java.util.function.Consumer;


public class LocalDummyTreeQueryClusterRunnerProxy implements TreeQueryClusterRunnerProxyInteface {

    private final CacheTypeEnum cacheTypeEnum;
    Map<Cluster, TreeQueryClusterRunner> treeQueryClusterRunnerMap = Maps.newHashMap();
    private final TreeQuerySetting treeQuerySetting;
    private final AvroSchemaHelper avroSchemaHelper;

    @Builder
    LocalDummyTreeQueryClusterRunnerProxy(CacheTypeEnum cacheTypeEnum, TreeQuerySetting treeQuerySetting, AvroSchemaHelper avroSchemaHelper){
        this.treeQuerySetting = treeQuerySetting;
        this.avroSchemaHelper = avroSchemaHelper;
        this.cacheTypeEnum = cacheTypeEnum;
    }

    @Override
    public void process(Node rootNode, Consumer<StatusTreeQueryCluster> statusCallback) {
        final Cluster cluster = rootNode.getCluster();
        treeQueryClusterRunnerMap.putIfAbsent(cluster, createNewDummyLocalRunner(cluster));
        TreeQueryClusterRunner treeQueryClusterRunner = treeQueryClusterRunnerMap.get(cluster);

        treeQueryClusterRunner.runQueryTreeNetwork(rootNode, statusCallback);
    }

    TreeQueryClusterRunner createNewDummyLocalRunner(Cluster cluster){

        TreeQueryClusterRunner treeQueryClusterRunner = TreeQueryClusterRunnerImpl.builder()
                                .beamCacheOutputBuilder(BeamCacheOutputBuilder.builder()
                                        .cacheTypeEnum(cacheTypeEnum)
                                        .treeQuerySetting(treeQuerySetting)
                                        .build())
                                .cacheTypeEnum(cacheTypeEnum)
                                .avroSchemaHelper(avroSchemaHelper)
                                .atCluster(cluster)
                                .treeQueryClusterRunnerProxyInteface(this)
                                .build();
        return treeQueryClusterRunner;
    }
}
