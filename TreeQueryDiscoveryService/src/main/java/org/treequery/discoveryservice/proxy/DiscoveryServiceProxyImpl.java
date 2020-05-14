package org.treequery.discoveryservice.proxy;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.DiscoveryServiceInterface;
import org.treequery.discoveryservice.Exception.InterfaceMethodNotUsedException;
import org.treequery.discoveryservice.model.Location;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@Slf4j
public class DiscoveryServiceProxyImpl implements DiscoveryServiceInterface {
    HttpClient client = HttpClient.newHttpClient();
    AmazonDynamoDB dbClient = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "ap-east-1"))
            .build();
    DynamoDB dynamoDB = new DynamoDB(dbClient);
    Table table = dynamoDB.getTable("ServiceMapping");

    @Override
    public void registerCacheResult(String hashId, Cluster cluster) {
        try {
            System.out.println("Adding a new item...");
            PutItemOutcome outcome = table
                    .putItem(new Item().withPrimaryKey("avro", hashId).withString("cluster", cluster.getClusterName()));
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception e) {
            System.err.println("Unable to add item: " + hashId);
            System.err.println(e.getMessage());
        }
    }

    @Override
    public Cluster getCacheResultCluster(String hashId) {
        GetItemSpec spec = new GetItemSpec().withPrimaryKey("avro", hashId);
        Cluster cluster = null;

        try {
            System.out.println("Attempting to read the item...");
            Item outcome = table.getItem(spec);
            System.out.println("GetItem succeeded: " + outcome);
            cluster = Cluster.builder()
                    .clusterName(outcome.getString("cluster"))
                    .build();
        } catch (Exception e) {
            System.err.println("Unable to read item: " + hashId + " ");
            System.err.println(e.getMessage());
        }
        return cluster;
    }

    @Override
    public void registerCluster(Cluster cluster, String address, int port) {
        throw new InterfaceMethodNotUsedException("registerCluster");
    }

    @Override
    public Location getClusterLocation(Cluster cluster) {
        Location location = null;

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(String.format("http://localhost:8082/service-instances/%s", cluster.getClusterName().toUpperCase())))
                    .build();

            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            location = objectMapper.readValue(response.body(), Location.class);
        } catch (Exception ex) {
            log.error("Exception in getClusterLocation(): ", ex);
        }
        return location;
    }
}
