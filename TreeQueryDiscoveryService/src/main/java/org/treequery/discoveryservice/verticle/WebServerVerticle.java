package org.treequery.discoveryservice.verticle;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.discoveryservicestatic.model.Location;

@RequiredArgsConstructor
public class WebServerVerticle extends AbstractVerticle {
    @NonNull private final DiscoveryServiceInterface ds;
    private final int PORT;

    @Override
    public void start(Future<Void> fut) {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.post("/registerCluster").handler(routingContext -> {
            JsonObject obj = routingContext.getBodyAsJson();
            Cluster cluster = Cluster.builder().clusterName(obj.getString("cluster")).build();
            String address = obj.getString("address");
            int port = obj.getInteger("port");
            ds.registerCluster(cluster, address, port);

            HttpServerResponse response = routingContext.response();
            response.putHeader("content-type", "application/json")
                    .end(Json.encodePrettily(Location.builder().address(address).port(port).build()));
        });

        router.get("/getClusterLocation/:service").handler(routingContext -> {
            String name = routingContext.request().getParam("service");
            Location location = ds.getClusterLocation(Cluster.builder().clusterName(name).build());

            HttpServerResponse response = routingContext.response();
            response.putHeader("content-type", "application/json")
                    .end(Json.encodePrettily(location));
        });

        router.post("/registerCacheResult").handler(routingContext -> {
            JsonObject obj = routingContext.getBodyAsJson();
            Cluster cluster = Cluster.builder().clusterName(obj.getString("cluster")).build();
            String identifier = obj.getString("identifier");
            ds.registerCacheResult(identifier, cluster);

            HttpServerResponse response = routingContext.response();
            response.putHeader("content-type", "application/json")
                    .end(Json.encodePrettily(Cluster.builder().clusterName(cluster.getClusterName()).build()));
        });

        router.get("/getCacheResultCluster/:identifier").handler(routingContext -> {
            String identifier = routingContext.request().getParam("identifier");
            Cluster cluster = ds.getCacheResultCluster(identifier);

            HttpServerResponse response = routingContext.response();
            response.putHeader("content-type", "application/json")
                    .end(Json.encodePrettily(cluster));
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
