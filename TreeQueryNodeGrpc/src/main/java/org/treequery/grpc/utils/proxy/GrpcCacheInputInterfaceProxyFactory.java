package org.treequery.grpc.utils.proxy;

import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.beam.cache.TreeQueryCacheProxy;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.utils.proxy.CacheInputInterfaceProxyFactory;

public class GrpcCacheInputInterfaceProxyFactory implements CacheInputInterfaceProxyFactory {
    @Override
    public CacheInputInterface getDefaultCacheInterface(TreeQuerySetting treeQuerySetting, DiscoveryServiceInterface discoveryServiceInterface) {
        return TreeQueryCacheProxy.builder()
                .discoveryServiceInterface(discoveryServiceInterface)
                .build();
    }
}
