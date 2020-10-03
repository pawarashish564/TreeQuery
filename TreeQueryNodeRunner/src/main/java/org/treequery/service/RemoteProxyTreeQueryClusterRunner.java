package org.treequery.service;

import lombok.extern.slf4j.Slf4j;
import org.treequery.model.Node;
import org.treequery.service.proxy.TreeQueryClusterRunnerProxyInterface;
import org.treequery.utils.AppExceptionHandler;

import java.util.function.Consumer;

@Slf4j
public class RemoteProxyTreeQueryClusterRunner implements TreeQueryClusterRunner{

    TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface;

    @Override
    public void runQueryTreeNetwork(Node node, Consumer<StatusTreeQueryCluster> statusCallback) {
        try {
            treeQueryClusterRunnerProxyInterface.runQueryTreeNetwork(
                    node, statusCallback
            );
        }catch (Throwable th){
            log.error(th.getMessage());
            AppExceptionHandler.feedBackException2Client(
                    statusCallback, node,
                    th.getMessage(),
                    StatusTreeQueryCluster.QueryTypeEnum.FAIL
            );
        }
    }

    @Override
    public void setTreeQueryClusterRunnerProxyInterface(TreeQueryClusterRunnerProxyInterface treeQueryClusterRunnerProxyInterface) {
        throw new NoSuchMethodError("Not supported for new LocalTreeQueryClusterRunner");
    }
}
