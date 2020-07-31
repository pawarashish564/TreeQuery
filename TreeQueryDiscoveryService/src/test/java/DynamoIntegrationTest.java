import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.client.DynamoClient;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;
import org.treequery.discoveryservicestatic.model.Location;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamoIntegrationTest {
    private static DynamoDBProxyServer server;
    private static DiscoveryServiceProxyImpl serviceProxy;
    private final static String avroTableName = "ServiceMapping";
    private final static String clusterTableName = "ClusterLocationMapping";
    private final static String endpoint = "http://localhost:8000";

    private static final BasicAWSCredentials AWS_CREDENTIALS;
    private DynamoDB dynamoDB;

    static {
        // Your accesskey and secretkey
        AWS_CREDENTIALS = new BasicAWSCredentials(
                "xxxxxxxxxxxxxxxxxxxx",
                "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        );
    }

    @BeforeAll
    public static void setupClass() throws Exception {

        DynamoClient dynamoClient = new DynamoClient(endpoint, "us-west-2", AWS_CREDENTIALS);
        //DynamoDB dynamoDB = new DynamoDB(client);
        String port = "8000";
        server = ServerRunner.createServerFromCommandLineArgs(
                new String[]{"-inMemory", "-port", port});
        server.start();
        serviceProxy = new DiscoveryServiceProxyImpl(dynamoClient.getDynamoDB());
        createTestTable();
    }

    @AfterAll
    public static void teardownClass() throws Exception {
        server.stop();
    }

    private static void createTestTable() {
        try {
            serviceProxy.getDynamoDB().createTable(avroTableName,
                    Arrays.asList(new KeySchemaElement("avro", KeyType.HASH)),
                    Arrays.asList(new AttributeDefinition("avro", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));
            serviceProxy.getDynamoDB().createTable(clusterTableName,
                    Arrays.asList(new KeySchemaElement("cluster", KeyType.HASH)),
                    Arrays.asList(new AttributeDefinition("cluster", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }

    @Test
    public void whenRegisterCacheResult_thenCanBeRetrieved() {
        serviceProxy.registerCacheResult("Avro-Test", Cluster.builder().clusterName("TestCluster").build());
        assertEquals("TestCluster", serviceProxy.getCacheResultCluster("Avro-Test").getClusterName());
    }

    @Test
    public void whenRegisterNewCluster_thenCanBeRetrieved() {
        serviceProxy.registerCluster(Cluster.builder().clusterName("TestCluster").build(), "addressTest", 8080);
        assertEquals("addressTest", serviceProxy.getClusterLocation(Cluster.builder().clusterName("TestCluster").build()).getAddress());
        assertEquals(8080, serviceProxy.getClusterLocation(Cluster.builder().clusterName("TestCluster").build()).getPort());
    }

    @Test
    public void whenRegisterExistingCluster_thenCanBeRetrieved() {
        Cluster testCluster1 = Cluster.builder().clusterName("TestCluster1").build();
        serviceProxy.registerCluster( testCluster1, "addressTest1", 8080);
        serviceProxy.registerCluster(testCluster1, "addressTest2", 8082);
        //assertEquals(2, serviceProxy.getLocationHelper(Cluster.builder().clusterName("TestCluster1").build()).size());
        Location location = serviceProxy.getClusterLocation(testCluster1);
        assertAll(
                ()->assertEquals("addressTest2",location.getAddress()),
                ()->assertEquals(8082, location.getPort())
        );

    }
}