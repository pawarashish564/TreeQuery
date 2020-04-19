package org.treequery.utils.proxy;

import org.treequery.beam.cache.CacheInputInterface;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;

public class LocalCacheInputInterfaceProxyFactory implements CacheInputInterfaceProxyFactory {

    @Override
    public CacheInputInterface getDefaultCacheInterface(TreeQuerySetting treeQuerySetting, DiscoveryServiceInterface discoveryServiceInterface) {
        return LocalDummyCacheInputImpl.builder()
                .discoveryServiceInterface(discoveryServiceInterface)
                .treeQuerySetting(treeQuerySetting)
                .build();
    }
}
