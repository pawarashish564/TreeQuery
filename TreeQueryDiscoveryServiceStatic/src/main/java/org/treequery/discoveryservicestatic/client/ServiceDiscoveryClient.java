package org.treequery.discoveryservicestatic.client;

import org.codehaus.jackson.map.ObjectMapper;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.discoveryservicestatic.model.Location;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class ServiceDiscoveryClient implements DiscoveryServiceInterface {
    private HttpClient client;
    private String url;
    private ObjectMapper mapper;

    public ServiceDiscoveryClient(String url) {
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.url = url;
        this.mapper = new ObjectMapper();

//        TODO: Use Vertx WebCLient and resolve async issue
//        client = WebClient.create(vertx, new WebClientOptions()
//                .setDefaultHost(host)
//                .setDefaultPort(port));

    }

    @Override
    public void registerCacheResult(String identifier, Cluster cluster) {
        String json = new StringBuilder()
                .append("{")
                .append(String.format("\"identifier\":\"%s\", ", identifier))
                .append(String.format("\"clusterName\": \"%s\"", cluster.getClusterName()))
                .append("}").toString();

        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .uri(URI.create(url + "/registerCacheResult"))
                .header("Content-Type", "application/json")
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("HTTP Response Code: " + response.statusCode());
            System.out.println("HTTP Response Body: " + response.body());
            if (response.statusCode() == 200) {
                System.out.println("RegisterCacheResult successfully.");
            } else {
                System.err.println(String.format("Failed to registerCacheResult: %s (%s)", response.body(), response.statusCode()));
            }
        } catch (Exception ex) {
            System.err.println("Failed to registerCacheResult: " + ex);
        }

/* TODO: Use Vertx WebCLient and resolve async issue

        client.post("/registerCacheResult")
                .sendJsonObject(new JsonObject()
                        .put("identifier", identifier)
                        .put("clusterName", cluster.getClusterName()), ar -> {
                    if (ar.succeeded()) {
                        HttpResponse<Buffer> response = ar.result();
                        System.out.println("RegisterCacheResult successfully.");
                    } else {
                        System.err.println("Failed to registerCacheResult: " + ar.cause().getMessage());
                    }
                });

 */
    }

    @Override
    public Cluster getCacheResultCluster(String identifier) {
        Cluster cluster = null;
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(url + "/getCacheResultCluster/" + identifier))
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("HTTP Response Code: " + response.statusCode());
            System.out.println("HTTP Response Body: " + response.body());
            cluster = mapper.readValue(response.body(), Cluster.class);
            System.out.println("RegisterCacheResult successfully.");
        } catch (Exception ex) {
            System.err.println("Failed to registerCacheResult: " + ex);
        }
        return cluster;
/* TODO: Use Vertx WebCLient and resolve async issue

        AtomicReference<Cluster> cluster = null;
        client.get(String.format("/getCacheResultCluster/%s", identifier)).send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                cluster.set(Json.decodeValue(response.body().toString(), Cluster.class));
                System.out.println("GetCacheResultCluster successfully.");
            } else {
                System.err.println("Failed to registerCacheResult: " + ar.cause().getMessage());
            }
        });
        return cluster.get();

 */
    }

    @Override
    public void registerCluster(Cluster cluster, String address, int port) {
        String json = new StringBuilder()
                .append("{")
                .append(String.format("\"cluster\":\"%s\", ", cluster.getClusterName()))
                .append(String.format("\"address\": \"%s\", ", address))
                .append(String.format("\"port\": %s", port))
                .append("}").toString();

        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .uri(URI.create(url + "/registerCluster"))
                .header("Content-Type", "application/json")
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                System.out.println("RegisterCluster successfully.");
            } else {
                System.err.println(String.format("Failed to registerCluster: %s (%s)", response.body(), response.statusCode()));
            }
        } catch (Exception ex) {
            System.err.println("Failed to registerCacheResult: " + ex);
        }

/*  TODO: Use Vertx WebCLient and resolve async issue

        client.post("/registerCluster")
                .sendJsonObject(new JsonObject()
                        .put("cluster", cluster)
                        .put("address", address)
                        .put("port", port), ar -> {
                    if (ar.succeeded()) {
                        HttpResponse<Buffer> response = ar.result();
                        System.out.println("RegisterCluster successfully.");
                    } else {
                        System.err.println("Failed to registerCluster: " + ar.cause().getMessage());
                    }
                });

 */
    }

    @Override
    public Location getClusterLocation(Cluster cluster) {
        Location location = null;
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(url + "/getClusterLocation/" + cluster.getClusterName()))
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("HTTP Response Code: " + response.statusCode());
            System.out.println("HTTP Response Body: " + response.body());
            location = mapper.readValue(response.body(), Location.class);
            System.out.println("RegisterCacheResult successfully.");
        } catch (Exception ex) {
            System.err.println("Failed to registerCacheResult: " + ex);
        }
        return location;

/*        TODO: Use Vertx WebCLient and resolve async issue
        AtomicReference<Location> location = null;
        Promise<Location> location = Promise.promise();
        client.get(String.format("/getClusterLocation/%s", cluster)).send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                location.set(Json.decodeValue(response.body().toString(), Location.class));
                System.out.println("GetClusterLocation successfully.");
            } else {
                location.
                System.err.println("Failed to getClusterLocation: " + ar.cause().getMessage());
            }
        });
        return location;

 */
    }
}
