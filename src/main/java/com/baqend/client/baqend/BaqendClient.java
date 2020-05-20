package com.baqend.client.baqend;

import com.baqend.config.ConfigObject;
import com.baqend.client.Client;
import com.baqend.utils.HttpClient;
import com.baqend.workload.LoadData;
import com.baqend.workload.SingleDataSet;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class BaqendClient implements Client {

    private ConfigObject configObject;
    private WebSocketClient webSocketClient;
    private BaqendQueryBuilder baqendQueryBuilder;
    private BaqendRequestBuilder baqendRequestBuilder;

    public BaqendClient(ConfigObject configObject) throws URISyntaxException {
        this.configObject = configObject;
        try {
            webSocketClient = new WebSocketClient(new URI(configObject.baqendWebsocketUri));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        baqendQueryBuilder = new BaqendQueryBuilder();
        baqendRequestBuilder = new BaqendRequestBuilder();

        System.out.println("BaqendClient WebsocketURI: " + configObject.baqendWebsocketUri);
        System.out.println("BaqendClient HttpBaseURI: " + configObject.baqendHttpBaseUri);
    }

    public void subscribeQuery(String query) {
        String queryString = baqendQueryBuilder.translateQuery(webSocketClient.userSession.toString(), query);
        System.out.println("Performing Query: " + queryString);
        webSocketClient.sendMessage(queryString);
    }

    public void unsubscribeQuery() {
        try {
            webSocketClient.userSession.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setup(String table, LoadData loadData) {
        for (SingleDataSet singleDataSet : loadData.getLoad()) {
            HttpClient.getInstance().post(configObject.baqendHttpBaseUri + "/db/" + table,
                    baqendRequestBuilder.composeRequestString(table, singleDataSet.getUuid().toString(), singleDataSet.getData())
            );
        }
        HttpClient.getInstance().stop();
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
    public void cleanUp(String table) {
        HttpClient.getInstance().stop();
//        MongoClient mongoClient = new MongoClient();
//        MongoDatabase db = mongoClient.getDatabase("local");
//        MongoCollection<Document> collection = db.getCollection(table);
//        collection.drop();
    }
}
