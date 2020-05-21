package org.treequery.grpc.utils.proxy;

import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.beam.cache.PageCacheProxy;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.utils.proxy.CacheInputInterfaceProxyFactory;

public class GrpcCacheInputInterfaceProxyFactory implements CacheInputInterfaceProxyFactory {
    @Override
    public CacheInputInterface getDefaultCacheInterface(TreeQuerySetting treeQuerySetting, DiscoveryServiceInterface discoveryServiceInterface) {
        return PageCacheProxy.builder()
                .discoveryServiceInterface(discoveryServiceInterface)
                .build();
    }
}
