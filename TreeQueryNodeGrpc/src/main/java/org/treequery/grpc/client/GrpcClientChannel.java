package org.treequery.grpc.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
@Slf4j
public class GrpcClientChannel {
    @Getter
    private  ManagedChannel channel;
    private final String host;
    private final int port;

    public GrpcClientChannel(String host, int port) {
        this.host = host;
        this.port = port;
        this.channel = createChannel(host, port);
    }

    public static ManagedChannel createChannel(String host, int port){
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build();
        return channel;
    }

    public void shutdown()  {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }catch(InterruptedException ie){
            log.error(ie.getMessage());
        }
    }
}
