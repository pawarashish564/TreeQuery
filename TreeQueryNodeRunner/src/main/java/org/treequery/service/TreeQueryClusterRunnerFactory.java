package org.treequery.service;
@FunctionalInterface
public interface TreeQueryClusterRunnerFactory {
    public TreeQueryClusterRunner createTreeQueryClusterRunner();
}
