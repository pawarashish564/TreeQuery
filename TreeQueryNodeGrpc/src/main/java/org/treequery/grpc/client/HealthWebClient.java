package org.treequery.grpc.client;

import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.treequery.proto.HealthCheckRequest;
import org.treequery.proto.HealthCheckResponse;
import org.treequery.proto.HealthCheckServiceGrpc;


@Slf4j
public class HealthWebClient {
    private final HealthCheckServiceGrpc.HealthCheckServiceBlockingStub blockingStub;

    private GrpcClientChannel grpcClientChannel;

    public HealthWebClient(String host, int port) {
        grpcClientChannel = new GrpcClientChannel(host, port);
        this.blockingStub = HealthCheckServiceGrpc.newBlockingStub(grpcClientChannel.getChannel());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                log.info("*** shutting down gRPC client since JVM is shutting down");
                grpcClientChannel.shutdown();
                log.info("*** client shut down");
            }
        });
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
        HealthWebClient healthWebClient = new HealthWebClient("localhost", PORT);
        boolean checkStatus = healthWebClient.healthCheck();
        log.info(String.format("Web client health check %b", checkStatus));
    }
}
