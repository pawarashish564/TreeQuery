package org.treequery.grpc.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.treequery.cluster.Cluster;
import org.treequery.config.TreeQuerySetting;
import org.treequery.discoveryservice.model.Location;
import org.treequery.grpc.server.Main;

import java.util.List;
import java.util.Random;

@RestController
public class ServiceDiscoveryController {
//    @Autowired
//    private DiscoveryClient discoveryClient;

    @RequestMapping("/location")
    public Location getAppLocation() {
//        List<ServiceInstance> instances = discoveryClient.getInstances(applicationName);
//        ServiceInstance server = instances.get(new Random().nextInt(instances.size() + 1));
//        List<Instance> serverList = new ArrayList<Instance>();
//        for (ServiceInstance instance : instances) {
//            serverList.add(Instance.builder().address(instance.getHost()).port(instance.getPort()).build());
//        }
//        Cluster.builder().clusterName(server.getServiceId()).build()
        return new Location(Main.treeQuerySetting.getServicehostname(),Main.treeQuerySetting.getServicePort());
    }
}
