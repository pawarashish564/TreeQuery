import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SimpleDiscoveryServiceTest {
    @Mock
    AmazonDynamoDB client;
    @Mock
    DynamoDB dynamoDB;
    @Mock
    Table table;
    @Mock
    PutItemOutcome outcome;
    @InjectMocks
    DiscoveryServiceProxyImpl proxy = new DiscoveryServiceProxyImpl();

    @Test
    public void whenGetCacheResultCluster_thenReturnValueFromDB() {
        when(table.getItem(any(GetItemSpec.class))).thenReturn(new Item().withString("cluster", "Cluster-A"));
        Cluster cluster = proxy.getCacheResultCluster("test");
        assertEquals("Cluster-A", cluster.getClusterName());
    }

    @Test
    public void whenRegisterCluster_thenWriteIntoDB() {
        when(table.putItem(any(Item.class))).thenReturn(outcome);
        when(outcome.getPutItemResult()).thenReturn(new PutItemResult());
        proxy.registerCacheResult("test", Cluster.builder().clusterName("test").build());
        verify(outcome).getPutItemResult();
        verify(table).putItem(new Item().withString("avro", "test").withString("cluster","test"));
    }
}
