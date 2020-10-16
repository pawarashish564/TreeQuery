package org.treequery.exception;

import org.treequery.service.StatusTreeQueryCluster;

public class FailClusterRunException extends RuntimeException{
    public FailClusterRunException(StatusTreeQueryCluster statusTreeQueryCluster){
        super(statusTreeQueryCluster.getDescription()+":"+statusTreeQueryCluster.getStatus());
    }
}
