import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.internal.StaticCredentialsProvider;
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
import org.treequery.discoveryservice.client.DynamoClient;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamoCRUDTest {
    private static DynamoDBProxyServer server;
    private static DiscoveryServiceProxyImpl serviceProxy;
    private final static String tableName = "ServiceMapping";
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