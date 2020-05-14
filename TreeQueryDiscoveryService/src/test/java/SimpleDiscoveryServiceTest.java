import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.Exception.InterfaceMethodNotUsedException;
import org.treequery.discoveryservice.model.Location;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@Slf4j
public class SimpleDiscoveryServiceTest {
    @Mock
    HttpResponse<String> response;
    @Mock
    HttpClient client;
    @Mock
    AmazonDynamoDB dbClient;
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

    @Test
    public void whenRegisterCluster_thenThrowInterfaceMethodNotUsedException(){
        assertThrows(InterfaceMethodNotUsedException.class, () -> proxy.registerCluster(Cluster.builder().clusterName("test").build(),"address", 123));
    }

    @SneakyThrows
    @Test
    public void whenGetClusterLocation_thenReturnServerLocation() {
        when(client.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        Location location = proxy.getClusterLocation(Cluster.builder().clusterName("TestCluster").build());
        assertNull(location);
    }
}
