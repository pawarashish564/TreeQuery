package org.treequery.serviceDiscoveryClient.controller;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.treequery.discoveryservice.model.Location;

@RestController
class ServiceInstanceRestController {
    @Autowired
    private EurekaClient discoveryClient;

    @RequestMapping("/service-instances/{applicationName}")
    public Location serviceInstanceByApplicationName(
            @PathVariable String applicationName) {
        final InstanceInfo instanceInfo = discoveryClient.getNextServerFromEureka(applicationName, false);
        return new Location(instanceInfo.getHostName(), instanceInfo.getPort());
    }
}
