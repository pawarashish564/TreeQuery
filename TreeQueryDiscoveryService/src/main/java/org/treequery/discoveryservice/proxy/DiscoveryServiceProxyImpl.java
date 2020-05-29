package org.treequery.discoveryservice.proxy;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.treequery.cluster.Cluster;
import org.treequery.discoveryservice.client.DynamoClient;
import org.treequery.discoveryservice.exception.InterfaceMethodNotUsedException;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;
import org.treequery.discoveryservicestatic.model.Location;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

@Slf4j
@NoArgsConstructor
public class DiscoveryServiceProxyImpl implements DiscoveryServiceInterface {
    private HttpClient client;
    private Table avroTable;
    private Table clusterTable;
    @Getter
    private DynamoDB dynamoDB;

    public DiscoveryServiceProxyImpl(String endpoint) {
        this(new DynamoClient(endpoint).getDynamoDB());
    }

    public DiscoveryServiceProxyImpl(DynamoDB dynamoDB){
        this.dynamoDB = dynamoDB;
        this.avroTable = dynamoDB.getTable("ServiceMapping");
        this.clusterTable = dynamoDB.getTable("ClusterLocationMapping");
        this.client = HttpClient.newHttpClient();
    }

    @Override
    public void registerCacheResult(String hashId, Cluster cluster) {
        try {
            System.out.println("Adding a new item...");
            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(new Item().withPrimaryKey("avro", hashId).withString("cluster", cluster.getClusterName()))
                    .withReturnValues(ReturnValue.ALL_OLD);
            PutItemOutcome outcome = avroTable
                    .putItem(putItemSpec);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception e) {
            System.err.println("Unable to add item: " + hashId);
            System.err.println(e);
        }
    }

    @Override
    public Cluster getCacheResultCluster(String hashId) {
        GetItemSpec spec = new GetItemSpec().withPrimaryKey("avro", hashId);
        Cluster cluster = null;

        try {
            System.out.println("Attempting to read the item...");
            Item outcome = avroTable.getItem(spec);
            System.out.println("GetItem succeeded: " + outcome);
            cluster = Cluster.builder()
                    .clusterName(outcome.getString("cluster"))
                    .build();
        } catch (Exception e) {
            System.err.println("Unable to read item: " + hashId + " ");
            System.err.println(e);
        }
        return cluster;
    }

    @Override
    public void registerCluster(Cluster cluster, String address, int port) {
        ArrayList<HashMap> locations = getLocationHelper(cluster);

        if (locations.size() == 0) {
            try {
                Map<String, Object> locationMap = new HashMap<String, Object>();
                locationMap.put("address", address);
                locationMap.put("port", port);

                List<Map<String, Object>> locationItems = new ArrayList<Map<String, Object>>();
                locationItems.add(locationMap);

                System.out.println("Adding a new item...");
                PutItemSpec putItemSpec = new PutItemSpec()
                        .withItem(new Item().withPrimaryKey("cluster", cluster.getClusterName()).withList("location", locationItems))
                        .withReturnValues(ReturnValue.ALL_OLD);
                PutItemOutcome outcome = clusterTable
                        .putItem(putItemSpec);
                System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
            } catch (Exception e) {
                System.err.println("Unable to add item: Cluster " + cluster.getClusterName());
                System.err.println(e);
            }
        } else {
            try {
                System.out.println("Updating a new item...");
                HashMap<String, Object> map = new HashMap<>();
                map.put("address", address);
                map.put("port", port);
                locations.add(map);
                UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("cluster", cluster.getClusterName())
                        .withUpdateExpression("set #loc = :l")
                        .withNameMap(new NameMap().with("#loc", "location"))
                        .withValueMap(new ValueMap().withList(":l", locations))
                        .withReturnValues(ReturnValue.UPDATED_NEW);
                UpdateItemOutcome outcome = clusterTable
                        .updateItem(updateItemSpec);
                System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
            } catch (Exception e) {
                System.err.println("Unable to update item: Cluster " + cluster.getClusterName());
                System.err.println(e);
            }
        }

        /* TODO: Use Eureka to register Cluster
        throw new InterfaceMethodNotUsedException("DiscoveryServiceProxyImpl.class registerCluster()");
         */
    }

    @Override
    public Location getClusterLocation(Cluster cluster) {
        ArrayList<HashMap> locations = getLocationHelper(cluster);
        Map<String, Object> ele = locations.get(new Random().nextInt(locations.size()));
        Location location = Location.builder().address(ele.get("address").toString()).port(Integer.parseInt(ele.get("port").toString())).build();
        return location;

        /* TODO: Use Eureka to get Cluster location
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
         */
    }

    public ArrayList<HashMap> getLocationHelper(Cluster cluster) {
        JsonArray jArr;
        ArrayList<HashMap> list = new ArrayList();

        try {
            System.out.println("Attempting to read the item...");
            Item outcome = clusterTable.getItem(new GetItemSpec().withPrimaryKey("cluster", cluster.getClusterName()));
            System.out.println("GetItem succeeded: " + outcome);
            jArr = JsonParser.parseString(outcome.getJSON("location")).getAsJsonArray();
            for (JsonElement json : jArr){
                HashMap<String, Object> map = new HashMap<>();
                map.put("address", json.getAsJsonObject().get("address").getAsString());
                map.put("port", json.getAsJsonObject().get("port").getAsInt());
                list.add(map);
            }
        } catch (Exception e) {
            System.err.println("Unable to read item: Cluster " + cluster.getClusterName());
            System.err.println(e);
        }
        return list;
    }
}