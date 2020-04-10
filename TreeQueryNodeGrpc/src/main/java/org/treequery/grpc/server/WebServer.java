package org.treequery.grpc.server;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class WebServer {
    private final int PORT;
    private final List<BindableService> bindableServiceList;
    private Server server;

    public void start() throws IOException {
        /* The port on which the server should run */
        ServerBuilder serverBuilder = ServerBuilder.forPort(PORT);
        bindableServiceList.forEach(
                bindableService -> serverBuilder.addService(bindableService)
        );
        server = serverBuilder
                .build()
                .start();
        log.info("Server started, listening on " + PORT);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                log.error("*** shutting down gRPC server since JVM is shutting down");
                WebServer.this.stop();
                log.error("*** server shut down");
            }
        });
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }


}
