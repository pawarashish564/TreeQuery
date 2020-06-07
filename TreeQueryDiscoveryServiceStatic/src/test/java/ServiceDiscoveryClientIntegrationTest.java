import org.junit.jupiter.api.*;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservicestatic.client.ServiceDiscoveryClient;
import org.treequery.discoveryservicestatic.model.Location;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;
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
    public void whenRegisterCacheResult_thenReturnSuccessMsg() {
        SDClient.registerCacheResult("TestAvro", Cluster.builder().clusterName("TestCluster").build());
        assertThat(outContent.toString()).contains("RegisterCacheResult successfully");
    }

    @Test
    public void whenRegisterCluster_thenReturnSuccessMsg() {
        SDClient.registerCluster(Cluster.builder().clusterName("TestCluster10").build(), "TestAddress", 123);
        assertThat(outContent.toString()).contains("RegisterCluster successfully");
    }

    @Test
    public void whenGetClusterLocation_thenReturnCorrectLocation() throws Exception {
        Location location = SDClient.getClusterLocation(Cluster.builder().clusterName("TestCluster10").build());
        assertEquals(location.getAddress(), "TestAddress");
        assertEquals(location.getPort(), 123);
    }

    @Test
    public void whenGetCacheResultCluster_thenReturnCorrectCluster() {
        Cluster cluster = SDClient.getCacheResultCluster("Avro-Test");
        assertEquals(cluster.getClusterName(), "TestCluster");
    }
}
