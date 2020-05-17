import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.Exception.InterfaceMethodNotUsedException;
import org.treequery.discoveryservice.model.Location;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@ExtendWith(MockitoExtension.class)
public class SimpleDiscoveryServiceTest {
    @Mock
    HttpClient mockClient;
    @Mock
    DynamoDB dynamoDB;
    @Mock
    Table table;
    @Mock
    PutItemOutcome outcome;
    @InjectMocks
    DiscoveryServiceInterface proxy = new DiscoveryServiceProxyImpl();

    private static HttpClient httpClient;
    private static final String URL_ENDPOINT = "http://localhost:1080/service-instances/TestCluster";
    private static ClientAndServer mockServer;

    @BeforeAll
    public static void init() {
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        mockServer = startClientAndServer(1080);
        settingRules();
    }

    @AfterAll
    public static void stopServer() {
        mockServer.stop();
    }

    public static void settingRules() {
        mockServer.when(
                request()
                        .withMethod("GET")
                        .withPath("/service-instances/TestCluster"),
                exactly(1))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeaders(
                                        new Header("Content-Type", "application/json; charset=utf-8"),
                                        new Header("Cache-Control", "public, max-age=86400"))
                                .withBody("{ \"address\": \"127.0.0.1\", \"port\": 8080 }")
                                .withDelay(TimeUnit.SECONDS, 1)
                );
    }

    @Test
    public void whenGetCacheResultCluster_thenReturnValueFromDB() {
        when(table.getItem(any(GetItemSpec.class))).thenReturn(new Item().withString("cluster", "Cluster-A"));
        Cluster cluster = proxy.getCacheResultCluster("test");
        assertEquals("Cluster-A", cluster.getClusterName());
    }

    @Test
    public void whenRegisterCluster_thenWriteIntoDB() {
        when(table.putItem(any(PutItemSpec.class))).thenReturn(outcome);
        when(outcome.getPutItemResult()).thenReturn(new PutItemResult());
        proxy.registerCacheResult("test", Cluster.builder().clusterName("test").build());
        verify(outcome).getPutItemResult();
    }

    @Test
    public void whenRegisterCluster_thenThrowInterfaceMethodNotUsedException() {
        assertThrows(InterfaceMethodNotUsedException.class, () -> proxy.registerCluster(Cluster.builder().clusterName("test").build(), "address", 123));
    }

    @Test
    public void whenGetClusterLocation_thenReturnServerLocation() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(URL_ENDPOINT))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        when(mockClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        Location location = proxy.getClusterLocation(Cluster.builder().clusterName("TestCluster").build());
        assertEquals("127.0.0.1", location.getAddress());
        assertEquals(8080, location.getPort());
    }
}
