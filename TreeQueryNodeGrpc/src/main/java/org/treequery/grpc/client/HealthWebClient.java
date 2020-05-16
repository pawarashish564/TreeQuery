package org.treequery.grpc.client;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.treequery.proto.HealthCheckRequest;
import org.treequery.proto.HealthCheckResponse;
import org.treequery.proto.HealthCheckServiceGrpc;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


@Slf4j
public class HealthWebClient {
    private final HealthCheckServiceGrpc.HealthCheckServiceBlockingStub blockingStub;
    private final HealthCheckServiceGrpc.HealthCheckServiceStub stub;
    private GrpcClientChannel grpcClientChannel;

    public HealthWebClient(String host, int port) {
        grpcClientChannel = new GrpcClientChannel(host, port);
        this.blockingStub = HealthCheckServiceGrpc.newBlockingStub(grpcClientChannel.getChannel());
        this.stub = HealthCheckServiceGrpc.newStub(grpcClientChannel.getChannel());

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

    public boolean asyncHealthCheck(){
        final CountDownLatch finishLatch = new CountDownLatch(1);
        final AtomicBoolean checkOK = new AtomicBoolean(false);
        HealthCheckRequest req = HealthCheckRequest.newBuilder().build();
        HealthCheckResponse res=null;
        stub.check(req, new StreamObserver<HealthCheckResponse>() {
            @Override
            public void onNext(HealthCheckResponse res) {
                log.info("health check status:"+res.getStatus());
                checkOK.set(res.getStatus() == HealthCheckResponse.ServingStatus.SERVING);
            }

            @Override
            public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                log.warn("Failed in healthcheck {0}", status);
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                finishLatch.countDown();
            }
        });
        try {
            finishLatch.await(1, TimeUnit.MINUTES);
        }catch(InterruptedException ie){
            log.error(ie.getMessage());
            return false;
        }
        return checkOK.get();
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
