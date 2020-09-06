package org.treequery.discoveryservice.client;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import lombok.Getter;

@Getter
public class DynamoClient {
    private DynamoDB dynamoDB;
    private AmazonDynamoDB amazonDynamoDB;

    static AmazonDynamoDBClientBuilder getBuilder(String endpoint, String region){
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endpoint, region)
                );
    }

    public DynamoClient(String endpoint, String region, BasicAWSCredentials basicAWSCredentials){
        this.amazonDynamoDB = getBuilder(endpoint, region)
                .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
                .build();
        this.dynamoDB = new DynamoDB(amazonDynamoDB);
    }
    public DynamoClient(String endpoint, String region){
        this.amazonDynamoDB = getBuilder(endpoint, region).build();
        this.dynamoDB = new DynamoDB(amazonDynamoDB);
    }
}