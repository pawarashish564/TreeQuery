package org.treequery.utils.proxy;

import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.utils.TreeQuerySettingHelper;

public class TreeQueryClusterAvroCacheProxyFactory {
    public static TreeQueryClusterAvroCacheInterface getDefaultCacheInterface(TreeQuerySetting treeQuerySetting, DiscoveryServiceInterface discoveryServiceInterface){
        return LocalDummyTreeQueryClusterAvroCacheImpl.builder()
                .discoveryServiceInterface(discoveryServiceInterface)
                .treeQuerySetting(treeQuerySetting)
                .build();

    }
}
