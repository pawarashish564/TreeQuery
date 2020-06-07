import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import org.junit.Ignore;
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
import org.treequery.discoveryservice.exception.InterfaceMethodNotUsedException;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;
import org.treequery.discoveryservicestatic.model.Location;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@ExtendWith(MockitoExtension.class)
public class DynamoDiscoveryServiceUnitTest {
    @Mock
    HttpClient mockClient;
    @Mock
    DynamoDB dynamoDB;
    @Mock
    Table arvoTable;
    @Mock
    Table clusterTable;
    @Mock
    PutItemOutcome poutcome;
    @Mock
    UpdateItemOutcome uoutcome;
    @InjectMocks
    DiscoveryServiceProxyImpl proxy = new DiscoveryServiceProxyImpl();

    private static HttpClient httpClient;
    private static final String URL_ENDPOINT = "http://localhost:1080/service-instances/TestCluster";
    private static ClientAndServer mockServer;

    @BeforeAll
    public static void init() {
        /* TODO: usable for Eureka module
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        mockServer = startClientAndServer(1080);
        settingRules();

         */
    }

    @AfterAll
    public static void stopServer() {
        /* TODO: usable for Eureka module
        mockServer.stop();

         */
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
        when(arvoTable.getItem(any(GetItemSpec.class))).thenReturn(new Item().withString("cluster", "Cluster-A"));
        Cluster cluster = proxy.getCacheResultCluster("test");
        assertEquals("Cluster-A", cluster.getClusterName());
    }

    @Test
    public void whenRegisterCacheCluster_thenWriteIntoDB() {
        when(arvoTable.putItem(any(PutItemSpec.class))).thenReturn(poutcome);
        when(poutcome.getPutItemResult()).thenReturn(new PutItemResult());
        proxy.registerCacheResult("test", Cluster.builder().clusterName("test").build());
        verify(poutcome).getPutItemResult();
        verify(arvoTable).putItem(any(PutItemSpec.class));
    }

    @Ignore
    public void whenRegisterClusterLocation_thenThrowInterfaceMethodNotUsedException() {
        assertThrows(InterfaceMethodNotUsedException.class, () -> proxy.registerCluster(Cluster.builder().clusterName("test").build(), "address", 123));
    }

    @Test
    public void whenRegisterNewClusterLocation_thenWriteIntoDB() {
        ArrayList<HashMap> testList = new ArrayList<>();

        when(clusterTable.putItem(any(PutItemSpec.class))).thenReturn(poutcome);
        when(poutcome.getPutItemResult()).thenReturn(new PutItemResult());
        when(clusterTable.getItem(any(GetItemSpec.class))).thenReturn(new Item().withList("location", testList));
        proxy.registerCluster(Cluster.builder().clusterName("test").build(), "addressTest", 123);
        verify(poutcome).getPutItemResult();
        verify(clusterTable).putItem(any(PutItemSpec.class));
    }

    @Test
    public void whenRegisterExistingClusterLocation_thenWriteIntoDB() {
        ArrayList<HashMap> testList1 = new ArrayList<>();
        HashMap<String, Object> testMap = new HashMap<>();
        testMap.put("address", "addressTest");
        testMap.put("port", 123);
        testList1.add(testMap);

        when(clusterTable.updateItem(any(UpdateItemSpec.class))).thenReturn(uoutcome);
        when(uoutcome.getItem()).thenReturn(new Item());
        when(clusterTable.getItem(any(GetItemSpec.class))).thenReturn(new Item().withList("location", testList1));
        proxy.registerCluster(Cluster.builder().clusterName("test").build(), "addressTest", 123);
        verify(uoutcome).getItem();
        verify(clusterTable).updateItem(any(UpdateItemSpec.class));
    }


    @Ignore
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

    @Test
    public void whenGetClusterLocation_thenReturnSingleServerLocation() {
        ArrayList<HashMap> testList = new ArrayList<>();
        HashMap<String, Object> testMap = new HashMap<>();
        testMap.put("address", "addressTest");
        testMap.put("port", 123);
        testList.add(testMap);

        when(clusterTable.getItem(any(GetItemSpec.class))).thenReturn(new Item().withList("location", testList));
        Location location = proxy.getClusterLocation(Cluster.builder().clusterName("test").build());
        assertEquals("addressTest", location.getAddress());
        assertEquals(123, location.getPort());
    }

    @Test
    public void whenCallGetLocationHelper_thenReturnListofLocations() {
        Map<String, Object> locationMap = new HashMap<String, Object>();
        locationMap.put("address", "addressTest");
        locationMap.put("port", 123);

        List<Map<String, Object>> locationItems = new ArrayList<>();
        locationItems.add(locationMap);

        when(clusterTable.getItem(any(GetItemSpec.class))).thenReturn(new Item().withList("location", locationItems));
        ArrayList<HashMap> locations = proxy.getLocationHelper(Cluster.builder().clusterName("test").build());
        assertEquals(1, locations.size());
    }
}