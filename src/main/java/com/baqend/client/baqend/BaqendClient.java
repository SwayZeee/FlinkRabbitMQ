package com.baqend.client.baqend;

import com.baqend.config.ConfigObject;
import com.baqend.core.LatencyMeasurement;
import com.baqend.client.Client;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

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
        try {
            StringEntity stringEntity = new StringEntity(baqendRequestBuilder.composeRequestString(table, key, values, transactionID));

            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(configObject.baqendHttpBaseUri + "/db/" + table);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            httpPost.setEntity(stringEntity);
            httpClient.execute(httpPost);
            httpClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void update(String table, String key, HashMap<String, String> values, UUID transactionID) {
        try {
            StringEntity stringEntity = new StringEntity(baqendRequestBuilder.composeRequestString(table, key, values, transactionID));

            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPut httpPut = new HttpPut(configObject.baqendHttpBaseUri + "/db/" + table + "/" + key);
            httpPut.setHeader("Accept", "application/json");
            httpPut.setHeader("Content-type", "application/json");
            httpPut.setEntity(stringEntity);
            httpClient.execute(httpPut);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(String table, String key, UUID transactionID) {
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpDelete httpDelete = new HttpDelete(configObject.baqendHttpBaseUri + "/db/" + table + "/" + key);
            httpClient.execute(httpDelete);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanUp() {

    }
}
