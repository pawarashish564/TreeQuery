import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.verticle.WebServerVerticle;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.discoveryservicestatic.model.Location;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
public class VertxRestDiscoveryServiceUnitTest {
    private Vertx vertx;
    private WebClient client;
    @Mock
    private static DiscoveryServiceInterface ds;

    @BeforeEach
    public void setup(VertxTestContext context) {
        vertx = Vertx.vertx();
        Verticle webVerticle = new WebServerVerticle(ds, 8082);
        vertx.deployVerticle(webVerticle, context.completing());
        client = WebClient.create(vertx, new WebClientOptions()
                .setDefaultHost("localhost")
                .setDefaultPort(8082)
        );
    }

    @AfterEach
    public void close(VertxTestContext context) {
        vertx.close(context.completing());
    }

    @Test
    public void checkWeCanRegisterCluster(VertxTestContext context) {
        client.post("/registerCluster")
                .sendJsonObject(new JsonObject()
                        .put("cluster", "TestCluster")
                        .put("address", "TestAddress")
                        .put("port", 123), ar -> {
                    if (ar.succeeded()) {
                        HttpResponse<Buffer> response = ar.result();
                        Location location = Json.decodeValue(response.body().toString(), Location.class);
                        context.verify(() -> {
                            assertEquals(response.statusCode(), 200);
                            assertTrue(response.headers().get("content-type").contains("application/json"));
                            assertEquals(location.getAddress(), "TestAddress");
                            assertEquals(location.getPort(), 123);
                        });
                        context.completeNow();
                    } else {
                        context.failNow(ar.cause());
                    }
                });
    }

    @Test
    public void checkWeCanGetClusterLocation(VertxTestContext context) {
        when(ds.getClusterLocation(any(Cluster.class))).thenReturn(Location.builder().address("TestAddress").port(123).build());
        client.get("/getClusterLocation/TestCluster").send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                Location location = Json.decodeValue(response.body().toString(), Location.class);
                context.verify(() -> {
                    assertEquals(response.statusCode(), 200);
                    assertTrue(response.headers().get("content-type").contains("application/json"));
                    assertEquals(location.getAddress(), "TestAddress");
                    assertEquals(location.getPort(), 123);
                });
                context.completeNow();
            } else {
                context.failNow(ar.cause());
            }
        });
    }

    @Test
    public void checkWeCanRegisterCacheResult(VertxTestContext context) {
        client.post("/registerCacheResult")
                .sendJsonObject(new JsonObject()
                        .put("identifier", "TestAvro")
                        .put("cluster", "TestCluster"), ar -> {
                    if (ar.succeeded()) {
                        HttpResponse<Buffer> response = ar.result();
                        Cluster cluster = Json.decodeValue(response.body().toString(), Cluster.class);
                        context.verify(() -> {
                            assertEquals(response.statusCode(), 200);
                            assertTrue(response.headers().get("content-type").contains("application/json"));
                            assertEquals(cluster.getClusterName(), "TestCluster");
                        });
                        context.completeNow();
                    } else {
                        context.failNow(ar.cause());
                    }
                });
    }

    @Test
    public void checkWeCanGetCacheResultCluster(VertxTestContext context) {
        when(ds.getCacheResultCluster(anyString())).thenReturn(Cluster.builder().clusterName("TestCluster").build());
        client.get("/getCacheResultCluster/TestAvro").send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                Cluster cluster = Json.decodeValue(response.body().toString(), Cluster.class);
                context.verify(() -> {
                    assertEquals(response.statusCode(), 200);
                    assertTrue(response.headers().get("content-type").contains("application/json"));
                    assertEquals(cluster.getClusterName(), "TestCluster");
                });
                context.completeNow();
            } else {
                context.failNow(ar.cause());
            }
        });
    }
}
