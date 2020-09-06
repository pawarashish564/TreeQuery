package org.treequery.discoveryservice.proxy;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
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

    public DiscoveryServiceProxyImpl(String endpoint , String region) {
        this(new DynamoClient(endpoint, region).getDynamoDB());
    }

    public DiscoveryServiceProxyImpl(DynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
        this.avroTable = getTable("ServiceMapping");
        this.clusterTable = getTable("ClusterLocationMapping");
        this.client = HttpClient.newHttpClient();
    }

    private Table getTable(String tableName) {
        Table table;
        try {
            TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
            log.info(String.format("Table %s is obtained. Status: %s.", tableDescription.getTableName(), tableDescription.getTableStatus()));
            table = dynamoDB.getTable(tableName);
        } catch (ResourceNotFoundException ex) {
            log.warn(String.format("%s table is not found in DynamoDB. A new table is created.", tableName), ex);
            switch (tableName) {
                case "ServiceMapping":
                    table = createTable(tableName, "avro");
                    break;
                case "ClusterLocationMapping":
                    table = createTable(tableName, "cluster");
                    break;
                default:
                    table = null;
                    log.error(String.format("By default, Table %s is not allowed to be created in DynamoDB.", tableName));
            }
        }
        return table;
    }

    private Table createTable(String tableName, String keyName) {
        Table newTable = null;
        try {
            List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(keyName).withAttributeType("S"));

            List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement().withAttributeName(keyName).withKeyType(KeyType.HASH));

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(5L)
                            .withWriteCapacityUnits(6L));

            Table table = dynamoDB.createTable(request);
            table.waitForActive();
            newTable = dynamoDB.getTable(tableName);
        } catch (Exception ex) {
            log.error(String.format("Unable to create %s table: ", tableName), ex);
        }
        return newTable;
    }

    @Override
    public void registerCacheResult(String hashId, Cluster cluster) {
        try {
            log.info("Adding a new item...");
            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(new Item().withPrimaryKey("avro", hashId).withString("cluster", cluster.getClusterName()))
                    .withReturnValues(ReturnValue.ALL_OLD);
            PutItemOutcome outcome = avroTable
                    .putItem(putItemSpec);
            log.info("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception ex) {
            log.error(String.format("Unable to add item %s: ", hashId), ex);
            throw ex;
        }
    }

    @Override
    public Cluster getCacheResultCluster(String hashId) {
        GetItemSpec spec = new GetItemSpec().withPrimaryKey("avro", hashId);
        Cluster cluster = null;

        try {
            log.info("Attempting to read the item...");
            Item outcome = avroTable.getItem(spec);
            log.info("GetItem succeeded: " + outcome);
            cluster = Cluster.builder()
                    .clusterName(outcome.getString("cluster"))
                    .build();
            return cluster;
        } catch (Exception ex) {
            log.error(String.format("Unable to read item %s: ", hashId), ex);
            throw ex;
        }
    }

    private  Map<String, Object> getLocationMapFromDynamoMap(Cluster cluster) {
        Map<String, Object> location = getSingleLocationHelper(cluster);
        Map<String,Object> LocMap = Optional.ofNullable((Map<String, Object>)location.get("location")).orElseThrow(
                ()->new NullPointerException("Location Map not found")
        );
        return LocMap;
    }
    @Override
    public void registerCluster(Cluster cluster, String address, int port) {
        //Should return One map only
        //ArrayList<HashMap> locations = getLocationHelper(cluster);
        Map<String,Object> LocMap = getLocationMapFromDynamoMap(cluster);
        if (LocMap.size() == 0) {
            try {
                Map<String, Object> locationMap = new HashMap<String, Object>();
                locationMap.put("address", address);
                locationMap.put("port", port);

                /*
                //We only keep one location, not a list
                //List<Map<String, Object>> locationItems = new ArrayList<Map<String, Object>>();
                //locationItems.add(locationMap);
                 */

                log.info("Adding a new item...");
                PutItemSpec putItemSpec = new PutItemSpec()
                        //.withItem(new Item().withPrimaryKey("cluster", cluster.getClusterName()).withList("location", locationItems))
                        .withItem(new Item().withPrimaryKey("cluster", cluster.getClusterName()).withMap("location", locationMap))
                        .withReturnValues(ReturnValue.ALL_OLD);
                PutItemOutcome outcome = clusterTable
                        .putItem(putItemSpec);
                log.info("PutItem succeeded:\n" + outcome.getPutItemResult());
            } catch (Exception ex) {
                log.error(String.format("Unable to add item Cluster %s: ", cluster.getClusterName()), ex);
                throw ex;
            }
        } else {
            try {
                log.info("Updating a new item...");
                HashMap<String, Object> map = new HashMap<>();
                map.put("address", address);
                map.put("port", port);
                //locations.add(map);
                UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("cluster", cluster.getClusterName())
                        .withUpdateExpression("set #loc = :l")
                        .withNameMap(new NameMap().with("#loc", "location"))
                        .withValueMap(new ValueMap().withMap(":l", map))
                        .withReturnValues(ReturnValue.UPDATED_NEW);
                UpdateItemOutcome outcome = clusterTable
                        .updateItem(updateItemSpec);
                log.info("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
            } catch (Exception ex) {
                log.error(String.format("Unable to update item Cluster %s: ", cluster.getClusterName()), ex);
                throw ex;
            }
        }

        /* TODO: Use Eureka to register Cluster
        throw new InterfaceMethodNotUsedException("DiscoveryServiceProxyImpl.class registerCluster()");
         */
    }

    @Override
    public Location getClusterLocation(Cluster cluster) {
        Map<String,Object> LocMap = getLocationMapFromDynamoMap(cluster);
        Location location =
                Location.builder()
                        .address(LocMap.get("address").toString())
                        .port(Integer.parseInt(LocMap.get("port").toString()))
                        .build();
        return location;
        /*
        try {
            ArrayList<HashMap> locations = getLocationHelper(cluster);
            Map<String, Object> ele = locations.get(new Random().nextInt(locations.size()));
            Location location = Location.builder().address(ele.get("address").toString()).port(Integer.parseInt(ele.get("port").toString())).build();
            return location;
        } catch (Exception ex) {
            throw ex;
        }*/

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

    public Map getSingleLocationHelper(Cluster cluster){
        try{
            log.info("Attempting to read item...");
            Item outcome = clusterTable.getItem(new GetItemSpec().withPrimaryKey("cluster", cluster.getClusterName()));
            return Optional.ofNullable(outcome).map(
                    (hasValueOutCome)->outcome.asMap()
            ).orElse(Map.of("location", Maps.newHashMap()));
        }catch(ProvisionedThroughputExceededException pe){
            //to be handled properly
            throw pe;
        }
    }
    /*
    public ArrayList<HashMap> getLocationHelper(Cluster cluster) {
        JsonArray jArr;
        ArrayList<HashMap> list = new ArrayList();

        try {
            log.info("Attempting to read the item...");
            Item outcome = clusterTable.getItem(new GetItemSpec().withPrimaryKey("cluster", cluster.getClusterName()));
            log.info("GetItem succeeded: " + outcome);
            jArr = JsonParser.parseString(outcome.getJSON("location")).getAsJsonArray();
            for (JsonElement json : jArr) {
                HashMap<String, Object> map = new HashMap<>();
                map.put("address", json.getAsJsonObject().get("address").getAsString());
                map.put("port", json.getAsJsonObject().get("port").getAsInt());
                list.add(map);
            }
        } catch (Exception ex) {
            log.error(String.format("Unable to read item Cluster %s.", cluster.getClusterName()));
        }
        return list;
    }*/
}