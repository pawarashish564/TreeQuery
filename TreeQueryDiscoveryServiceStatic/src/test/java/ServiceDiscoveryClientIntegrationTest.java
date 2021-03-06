import org.junit.jupiter.api.*;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservicestatic.client.ServiceDiscoveryClient;
import org.treequery.discoveryservicestatic.model.Location;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class ServiceDiscoveryClientIntegrationTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private static ServiceDiscoveryClient SDClient;

    @BeforeAll
    public static void init() {
        SDClient = new ServiceDiscoveryClient("http://localhost:8082");
    }

    @BeforeEach
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    @AfterEach
    public void restoreStreams() {
        System.setOut(originalOut);
    }

    @Test
    public void whenRegisterCacheResult_thenNoExceptionReturned() {
        assertDoesNotThrow(() -> SDClient.registerCacheResult("TestAvro",
                Cluster.builder().clusterName("TestCluster").build()));
    }

    @Test
    public void whenRegisterCluster_thenNoExceptionReturned() {
        assertDoesNotThrow(() -> SDClient.registerCluster(Cluster.builder().clusterName("TestCluster").build(),
                "TestAddress", 123));
    }

    @Test
    public void whenGetClusterLocation_thenReturnCorrectLocation() throws Exception {
        assertDoesNotThrow(() -> SDClient.registerCluster(Cluster.builder().clusterName("TestCluster").build(),
                "TestAddress", 123));
        Location location = SDClient.getClusterLocation(Cluster.builder().clusterName("TestCluster").build());
        assertEquals(location.getAddress(), "TestAddress");
        assertEquals(location.getPort(), 123);
    }

    @Test
    public void whenGetCacheResultCluster_thenReturnCorrectCluster() {
        assertDoesNotThrow(() -> SDClient.registerCacheResult("TestAvro",
                Cluster.builder().clusterName("TestCluster").build()));
        Cluster cluster = SDClient.getCacheResultCluster("TestAvro");
        assertEquals(cluster.getClusterName(), "TestCluster");
    }
}
