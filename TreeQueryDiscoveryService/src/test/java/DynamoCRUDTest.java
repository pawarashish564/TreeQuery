import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamoCRUDTest {
    private static DynamoDBProxyServer server;
    private static DiscoveryServiceProxyImpl serviceProxy;
    private final static String tableName = "ServiceMapping";
    private final static String endpoint = "http://localhost:8000";

    @BeforeAll
    public static void setupClass() throws Exception {
        String port = "8000";
        server = ServerRunner.createServerFromCommandLineArgs(
                new String[]{"-inMemory", "-port", port});
        server.start();
        serviceProxy = new DiscoveryServiceProxyImpl(endpoint);
        createTestTable();
    }

    @AfterAll
    public static void teardownClass() throws Exception {
        server.stop();
    }

    private static void createTestTable() {
        try {
            serviceProxy.getDynamoDB().createTable(tableName,
                    Arrays.asList(new KeySchemaElement("avro", KeyType.HASH)),
                    Arrays.asList(new AttributeDefinition("avro", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }

    @Test
    public void whenRegisterCluster_thenCanBeRetrieved() {
        serviceProxy.registerCacheResult("Avro-Test", Cluster.builder().clusterName("TestCluster").build());
        assertEquals("TestCluster", serviceProxy.getCacheResultCluster("Avro-Test").getClusterName());
    }
}