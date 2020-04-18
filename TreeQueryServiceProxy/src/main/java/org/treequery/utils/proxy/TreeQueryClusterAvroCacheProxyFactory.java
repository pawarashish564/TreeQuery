package org.treequery.utils.proxy;

import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;

public interface TreeQueryClusterAvroCacheProxyFactory {
    public TreeQueryClusterAvroCacheInterface getDefaultCacheInterface(TreeQuerySetting treeQuerySetting, DiscoveryServiceInterface discoveryServiceInterface);
}
