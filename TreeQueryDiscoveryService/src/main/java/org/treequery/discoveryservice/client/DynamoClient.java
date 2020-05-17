package org.treequery.discoveryservice.client;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import lombok.Getter;

@Getter
public class DynamoClient {
    private DynamoDB dynamoDB;
    private AmazonDynamoDB amazonDynamoDB;

    public DynamoClient(String endpoint){
        this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endpoint, "us-west-2")
                ).build();
        this.dynamoDB = new DynamoDB(amazonDynamoDB);
    }
}
