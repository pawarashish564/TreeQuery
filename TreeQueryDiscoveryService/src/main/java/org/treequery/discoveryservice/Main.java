package org.treequery.discoveryservice;

import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import org.treequery.discoveryservice.proxy.DiscoveryServiceProxyImpl;
import org.treequery.discoveryservice.verticle.WebServerVerticle;

public class Main {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        DiscoveryServiceProxyImpl ds = new DiscoveryServiceProxyImpl("https://dynamodb.us-west-2.amazonaws.com");
        Verticle webVerticle = new WebServerVerticle(ds);
        vertx.deployVerticle(webVerticle);
    }
}
