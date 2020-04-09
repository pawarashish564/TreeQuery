package org.treequery.grpc.server;

import io.grpc.BindableService;
import org.apache.avro.generic.GenericData;
import org.treequery.grpc.controller.TreeQuerySyncGrpcController;

import java.io.IOException;
import java.util.Arrays;

public class Main {
    public static void main(String [] args) throws IOException, InterruptedException {
        BindableService[] bindableServices = {new TreeQuerySyncGrpcController()};


        int PORT = 9001;
        WebServer webServer = new WebServer(PORT, Arrays.asList(bindableServices));
        webServer.start();
        webServer.blockUntilShutdown();
    }
}
