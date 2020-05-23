package com.baqend.clients.baqend;

import com.baqend.clients.Client;
import com.baqend.clients.baqend.helper.BaqendQueryBuilder;
import com.baqend.clients.baqend.helper.BaqendRequestBuilder;
import com.baqend.clients.baqend.helper.BaqendWebSocketClient;
import com.baqend.utils.httpclients.AHCAsyncHttpClient;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class BaqendClient implements Client {

    private final String BAQEND_WEBSOCKET_URI = "ws://localhost:8080/v1/events";
    private final String BAQEND_HTTP_BASE_URI = "http://localhost:8080/v1";

    private final BaqendQueryBuilder baqendQueryBuilder = new BaqendQueryBuilder();
    private final BaqendRequestBuilder baqendRequestBuilder = new BaqendRequestBuilder();

    private BaqendWebSocketClient baqendWebSocketClient;

    public BaqendClient() {
        try {
            baqendWebSocketClient = new BaqendWebSocketClient(new URI(BAQEND_WEBSOCKET_URI));
        } catch (IOException | TimeoutException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void subscribeQuery(UUID queryID, String query) {
        String queryString = baqendQueryBuilder.composeSubscribeQueryString(queryID.toString(), query);
        baqendWebSocketClient.sendMessage(queryString);
    }

    @Override
    public void unsubscribeQuery(UUID queryID) {
        String queryString = baqendQueryBuilder.composeUnsubscribeQueryString(queryID.toString());
        baqendWebSocketClient.sendMessage(queryString);
    }

    @Override
    public void insert(String table, String key, HashMap<String, String> values, UUID transactionID) {
        AHCAsyncHttpClient.getInstance().post(BAQEND_HTTP_BASE_URI + "/db/" + table,
                baqendRequestBuilder.composeRequestString(table, key, values, transactionID)
        );
    }

    @Override
    public void update(String table, String key, HashMap<String, String> values, UUID transactionID) {
        AHCAsyncHttpClient.getInstance().put(BAQEND_HTTP_BASE_URI + "/db/" + table + "/" + key,
                baqendRequestBuilder.composeRequestString(table, key, values, transactionID)
        );
    }

    @Override
    public void delete(String table, String key, UUID transactionID) {
        AHCAsyncHttpClient.getInstance().delete(BAQEND_HTTP_BASE_URI + "/db/" + table + "/" + key);
    }

    @Override
    public void cleanUp(String table) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("local");
        MongoCollection<Document> collection = db.getCollection(table);
        collection.drop();
    }

    @Override
    public void close() {
        baqendWebSocketClient.close();
        AHCAsyncHttpClient.getInstance().stop();
    }
}
