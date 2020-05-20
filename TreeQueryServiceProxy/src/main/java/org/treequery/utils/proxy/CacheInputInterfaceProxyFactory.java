package org.treequery.utils.proxy;

import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;

public interface CacheInputInterfaceProxyFactory {
    public CacheInputInterface getDefaultCacheInterface(TreeQuerySetting treeQuerySetting, DiscoveryServiceInterface discoveryServiceInterface);
}
