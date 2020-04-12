package org.treequery.service.proxy;

import lombok.Builder;
import org.apache.avro.generic.GenericRecord;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.PreprocessInput;
import org.treequery.service.ReturnResult;

import java.util.function.Consumer;

@Builder
public class LocalDummyTreeQueryClusterRunner implements TreeQueryClusterRunnerInterface {
    private final TreeQuerySetting treeQuerySetting;
    private final DiscoveryServiceInterface discoveryServiceInterface;



    @Override
    public ReturnResult process(Cluster cluster, TreeQueryRequest.RunMode runMode, PreprocessInput preprocessInput, boolean renewCache, long pageSize, long page, Consumer<GenericRecord> dataConsumer) {
        //For local run, cluster is dummy




        return null;
    }
}
