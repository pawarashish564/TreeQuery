package org.treequery.utils.proxy;

import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.utils.TreeQuerySettingHelper;

public class TreeQueryClusterAvroCacheProxyFactory {
    public static TreeQueryClusterAvroCacheInterface getDefaultCacheInterface(DiscoveryServiceInterface discoveryServiceInterface){
        TreeQuerySetting treeQuerySetting = TreeQuerySettingHelper.createFromYaml();
        return LocalDummyTreeQueryClusterAvroCacheImpl.builder()
                .discoveryServiceInterface(discoveryServiceInterface)
                .treeQuerySetting(treeQuerySetting)
                .build();

    }
}
