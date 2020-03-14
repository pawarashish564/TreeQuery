package io.exp.treequery.service;
@FunctionalInterface
public interface TreeQueryClusterRunnerFactory {
    public TreeQueryClusterRunner createTreeQueryClusterRunner();
}
