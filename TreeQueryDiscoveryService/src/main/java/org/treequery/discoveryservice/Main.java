package org.treequery.discoveryservice;

import com.amazonaws.auth.BasicAWSCredentials;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.treequery.discoveryservice.client.DynamoClient;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;
import org.treequery.discoveryservice.verticle.WebServerVerticle;
import org.treequery.discoveryservicestatic.DiscoveryServiceInterface;

import java.util.Optional;

@Slf4j
public class Main {
    static String endpointURLFormat = "https://dynamodb.%s.amazonaws.com";
    static int port = 8082;
    private static DiscoveryServiceInterface parseInputParamters(String[] args){
        Options options = new Options();
        Option regionInput = new Option("r", "region", true, "region of AWS dynamoDB");
        options.addOption(regionInput);

        Option accessKeyInput = new Option("a", "accessKey", true, "aws Access key");
        options.addOption(accessKeyInput);

        Option secretKeyInput = new Option("s", "secretKey", true, "aws secret key");
        options.addOption(secretKeyInput);
        Option endpointInput = new Option("e", "endpoint", true, "AWS DynamoDB endpoint");
        options.addOption(endpointInput);

        Option portInput = new Option("p", "port", true, "Discovery service port");
        options.addOption(portInput);


        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }
        port = Optional.ofNullable(cmd.getOptionValue("port")).map(
                (portStr)->Integer.parseInt(portStr)
        ).orElse(port);
        log.info(String.format("host service at %d", port));

        String region = Optional.ofNullable(cmd.getOptionValue("region")).orElseThrow(
                ()->new IllegalArgumentException("please provide region")
        );
        String endpoint = Optional.ofNullable(cmd.getOptionValue("endpoint"))
                .orElse(
                        Optional.ofNullable(region).map(
                                (value)->String.format(endpointURLFormat, value)
                        ).orElseThrow(()->
                                new IllegalArgumentException("without endpoint, please provide region"))
                );
        String accessKey = cmd.getOptionValue("accessKey");
        String secretKey = cmd.getOptionValue("secretKey");

        if (accessKey ==null || secretKey == null){
            return new DiscoveryServiceProxyImpl(endpoint, region);
        }else {
            BasicAWSCredentials AWS_CREDENTIALS = new BasicAWSCredentials(
                    accessKey,
                    secretKey
            );
            if (region == null){
                throw new IllegalArgumentException("require region to initialize the Basic AWS credential");
            }
            DynamoClient dynamoClient = new DynamoClient(endpoint, region, AWS_CREDENTIALS);
            return new DiscoveryServiceProxyImpl(dynamoClient.getDynamoDB());
        }
    }

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        DiscoveryServiceInterface ds = parseInputParamters(args);
        Verticle webVerticle = new WebServerVerticle(ds, port);
        vertx.deployVerticle(webVerticle);
    }
}
