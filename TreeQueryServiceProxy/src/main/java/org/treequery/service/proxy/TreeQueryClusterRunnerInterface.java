package org.treequery.service.proxy;

import org.apache.avro.generic.GenericRecord;
import org.treequery.cluster.Cluster;
//import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.PreprocessInput;
import org.treequery.service.ReturnResult;

import java.util.function.Consumer;

public interface TreeQueryClusterRunnerInterface {

    public ReturnResult process(Cluster cluster, TreeQueryRequest.RunMode runMode,
                                PreprocessInput preprocessInput,
                                boolean renewCache,
                                long pageSize,
                                long page,
                                Consumer<GenericRecord> dataConsumer);


}
