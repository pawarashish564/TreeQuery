import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.client.DynamoClient;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class DynamoCRUDTest {
    private static DynamoDBProxyServer server;
    private static DynamoDB dynamoDB;
    private static DiscoveryServiceInterface serviceProxy;
    private final static String tableName = "ServiceMapping";

    @BeforeAll
    public static void setupClass() throws Exception {
        System.setProperty("sqlite4java.library.path", "src/test/resources/libs/");
        String port = "8000";
        server = ServerRunner.createServerFromCommandLineArgs(
                new String[]{"-inMemory", "-port", port});
        server.start();
        dynamoDB = new DynamoClient("http://localhost:8000").getDynamoDB();
        createTestTable();
        serviceProxy = new DiscoveryServiceProxyImpl(dynamoDB);
    }

    @AfterAll
    public static void teardownClass() throws Exception {
        server.stop();
    }

    private static void createTestTable() {
        dynamoDB.createTable(tableName,
                Arrays.asList(new KeySchemaElement("avro", KeyType.HASH)),
                Arrays.asList(new AttributeDefinition("avro", ScalarAttributeType.S)),
                new ProvisionedThroughput(10L, 10L));
    }

    @Test
    public void whenRegisterCluster_thenCanBeRetrieved() {
        serviceProxy.registerCacheResult("Avro-Test", Cluster.builder().clusterName("TestCluster").build());
        assertEquals("TestCluster", serviceProxy.getCacheResultCluster("Avro-Test").getClusterName());
    }
}
