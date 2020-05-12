package com.baqend.client.baqend;

import com.baqend.config.ConfigObject;
import com.baqend.core.LatencyMeasurement;
import com.baqend.client.Client;
import com.baqend.utils.HttpClient;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.UUID;

public class BaqendClient implements Client {

    private final ConfigObject configObject;
    private final WebSocketClient webSocketClient;
    private final BaqendQueryBuilder baqendQueryBuilder;
    private final BaqendRequestBuilder baqendRequestBuilder;

    public BaqendClient(ConfigObject configObject) throws URISyntaxException {
        this.configObject = configObject;
        webSocketClient = new WebSocketClient(new URI(configObject.baqendWebsocketUri));
        baqendQueryBuilder = new BaqendQueryBuilder();
        baqendRequestBuilder = new BaqendRequestBuilder();

        System.out.println("BaqendClient WebsocketURI: " + configObject.baqendWebsocketUri);
        System.out.println("BaqendClient HttpBaseURI: " + configObject.baqendHttpBaseUri);
    }

    public void doQuery(String query) {
        String queryString = baqendQueryBuilder.translateQuery(webSocketClient.userSession.toString(), query);
        System.out.println("Performing Query: " + queryString);

        UUID uuid = UUID.randomUUID();
        LatencyMeasurement.getInstance().tick(uuid);
        LatencyMeasurement.getInstance().setInitialTick(uuid);
        webSocketClient.sendMessage(queryString);
    }

    public void setup() {
    }

    public void warmUp() {
    }

    @Override
    public void insert(String table, String key, HashMap<String, String> values, UUID transactionID) {
        HttpClient.getInstance().post(configObject.baqendHttpBaseUri + "/db/" + table,
                baqendRequestBuilder.composeRequestString(table, key, values, transactionID)
        );
    }

    @Override
    public void update(String table, String key, HashMap<String, String> values, UUID transactionID) {
        HttpClient.getInstance().put(configObject.baqendHttpBaseUri + "/db/" + table + "/" + key,
                baqendRequestBuilder.composeRequestString(table, key, values, transactionID)
        );
    }

    @Override
    public void delete(String table, String key, UUID transactionID) {
        HttpClient.getInstance().delete(configObject.baqendHttpBaseUri + "/db/" + table + "/" + key);
    }

    @Override
    public void cleanUp() {
        HttpClient.getInstance().stop();
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("local");
        MongoCollection<Document> collection = db.getCollection("Test");
        collection.drop();
    }
}
