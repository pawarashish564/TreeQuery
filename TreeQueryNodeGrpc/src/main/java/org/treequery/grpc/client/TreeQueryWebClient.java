package org.treequery.grpc.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.treequery.proto.HealthCheckRequest;
import org.treequery.proto.HealthCheckResponse;
import org.treequery.proto.TreeQueryServiceGrpc;

import java.util.concurrent.TimeUnit;


@Slf4j
public class TreeQueryWebClient {
    private final TreeQueryServiceGrpc.TreeQueryServiceBlockingStub blockingStub;
    private  ManagedChannel channel;

    public static ManagedChannel createChannel(String host, int port){
        return ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build();
    }

    public TreeQueryWebClient(String host, int port) {
        this.channel = createChannel(host, port);
        this.blockingStub = TreeQueryServiceGrpc.newBlockingStub(this.channel);

    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public boolean healthCheck(){
        HealthCheckRequest req = HealthCheckRequest.newBuilder().build();
        HealthCheckResponse res=null;
        try {
            res = blockingStub.check(req);
            log.info("health check status:"+res.getStatus());
            if(res.getStatus() == HealthCheckResponse.ServingStatus.SERVING){
                return true;
            }else{
                return false;
            }
        }catch(StatusRuntimeException se){
            log.info("unable to connect:"+se.getMessage());
            return false;
        } catch(Exception ex){
            log.warn("failed to connect:"+ex.getMessage());
            return false;
        }
    }

    public static void main(String [] args) throws Exception{
        int PORT = 9001;
        TreeQueryWebClient treeQueryWebClient = new TreeQueryWebClient("localhost", PORT);
        boolean checkStatus = treeQueryWebClient.healthCheck();
        log.info(String.format("Web client health check %b", checkStatus));
    }
}
