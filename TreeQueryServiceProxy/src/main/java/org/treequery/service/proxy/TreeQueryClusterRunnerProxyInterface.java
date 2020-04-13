package org.treequery.service.proxy;

import org.apache.avro.generic.GenericRecord;
import org.treequery.cluster.Cluster;
//import org.treequery.grpc.service.TreeQueryBeamServiceHelper;
import org.treequery.model.Node;
import org.treequery.proto.TreeQueryRequest;
import org.treequery.service.PreprocessInput;
import org.treequery.service.ReturnResult;
import org.treequery.service.StatusTreeQueryCluster;

import java.util.function.Consumer;

public interface TreeQueryClusterRunnerProxyInterface {

    public void process(Node rootNode, Consumer<StatusTreeQueryCluster> statusCallback);


}
