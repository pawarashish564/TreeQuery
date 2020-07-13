package org.treequery.discoveryservice.verticle;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.discoveryservicestatic.model.Location;

@RequiredArgsConstructor
public class WebServerVerticle extends AbstractVerticle {
    @NonNull
    private final DiscoveryServiceInterface ds;
    private final int PORT;

    @Override
    public void start(Future<Void> fut) {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        HealthCheckHandler hcHandler = HealthCheckHandler.create(vertx);
        hcHandler.register("endpoints-process", future -> future.complete(Status.OK()));

        router.route("/healthCheck").handler(hcHandler);

        router.post("/registerCluster").handler(routingContext -> {
            HttpServerResponse response = routingContext.response();

            try {
                JsonObject obj = routingContext.getBodyAsJson();
                Cluster cluster = Cluster.builder().clusterName(obj.getString("cluster")).build();
                String address = obj.getString("address");
                int port = obj.getInteger("port");
                ds.registerCluster(cluster, address, port);
                response.putHeader("content-type", "application/json")
                        .end(Json.encodePrettily(Location.builder().address(address).port(port).build()));
            } catch (Exception ex) {
                response.setStatusCode(500).end("Unable to registerCluster. Exception: " + ex);
            }
        });

        router.get("/getClusterLocation/:service").handler(routingContext -> {
            HttpServerResponse response = routingContext.response();

            try {
                String name = routingContext.request().getParam("service");
                Location location = ds.getClusterLocation(Cluster.builder().clusterName(name).build());
                response.putHeader("content-type", "application/json")
                        .end(Json.encodePrettily(location));
            } catch (Exception ex) {
                response.setStatusCode(500).end("Unable to getClusterLocation. Exception: " + ex);
            }
        });

        router.post("/registerCacheResult").handler(routingContext -> {
            HttpServerResponse response = routingContext.response();

            try {
                JsonObject obj = routingContext.getBodyAsJson();
                Cluster cluster = Cluster.builder().clusterName(obj.getString("cluster")).build();
                String identifier = obj.getString("identifier");
                ds.registerCacheResult(identifier, cluster);
                response.putHeader("content-type", "application/json")
                        .end(Json.encodePrettily(Cluster.builder().clusterName(cluster.getClusterName()).build()));
            } catch (Exception ex) {
                response.setStatusCode(500).end("Unable to registerCacheResult. Exception: " + ex);
            }
        });

        router.get("/getCacheResultCluster/:identifier").handler(routingContext -> {
            HttpServerResponse response = routingContext.response();

            try {
                String identifier = routingContext.request().getParam("identifier");
                Cluster cluster = ds.getCacheResultCluster(identifier);
                response.putHeader("content-type", "application/json")
                        .end(Json.encodePrettily(cluster));
            } catch (Exception ex) {
                response.setStatusCode(500).end("Unable to getCacheResultCluster. Exception: " + ex);
            }
        });



        vertx.createHttpServer()
                .requestHandler(router::accept)
                .listen(
                        config().getInteger("http.port", PORT),
                        result -> {
                            if (result.succeeded()) {
                                fut.complete();
                            } else {
                                fut.fail(result.cause());
                            }
                        }
                );
    }
}
