package com.baqend.client;

import com.baqend.ConfigObject;
import com.baqend.LatencyMeasurement;
import com.baqend.utils.WebSocketClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.UUID;

public class BaqendClient implements Client {

    private ConfigObject configObject;
    private WebSocketClient webSocketClient;
    private ArrayList<String> data;

    public BaqendClient(ConfigObject configObject) throws URISyntaxException {
        this.configObject = configObject;

        webSocketClient = new WebSocketClient(new URI(configObject.baqendWebsocketUri));
        System.out.println("BaqendClient WebsocketURI: " + configObject.baqendWebsocketUri);
        System.out.println("BaqendClient HttpBaseURI: " + configObject.baqendHttpBaseUri);

        data = new ArrayList<String>();
    }

    public void doQuery(String query) {
        /*String queryString1 = "{\n" +
                "  \"id\": \"" + webSocketClient.userSession.getId().toString() + "\",\n" +
                "  \"type\": \"subscribe\",\n" +
                "  \"token\": null,\n" +
                "  \"initial\": true,\n" +
                "  \"bucket\": \"Test\",\n" +
                "  \"query\": \"{\\\"testName\\\": \\\"Patrick\\\"}\",\n" +
                "  \"operations\": [\n" +
                "    \"any\"\n" +
                "  ],\n" +
                "  \"matchTypes\": [\n" +
                "    \"all\"\n" +
                "  ]\n}";*/
        String queryString = "{\n" +
                "  \"id\": \"" + webSocketClient.getUserSession().toString() + "\",\n" +
                "  \"type\": \"subscribe\",\n" +
                "  \"token\": null,\n" +
                "  \"initial\": true,\n" +
                "  \"bucket\": \"Test\",\n" +
                "  \"query\": \"" + query + "\",\n" +
                "  \"operations\": [\n" +
                "    \"any\"\n" +
                "  ],\n" +
                "  \"matchTypes\": [\n" +
                "    \"all\"\n" +
                "  ]\n}";
        System.out.println("Performing Query: " + queryString);
        webSocketClient.sendMessage(queryString);
        UUID uuid = UUID.randomUUID();
        LatencyMeasurement.getInstance().tick(uuid);
        LatencyMeasurement.getInstance().setInitialTick(uuid);
    }

    public void setup() {
    }

    public void warmUp() {
    }

    public void updateData(UUID uuid) throws IOException {
        // Refactor: move http client so new file and setup URL in setup
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPut httpPut = new HttpPut(configObject.baqendHttpBaseUri + "/db/Test/7766b85b-3b35-4258-b8da-48845cd0d92d");
            httpPut.setHeader("Accept", "application/json");
            httpPut.setHeader("Content-type", "application/json");
            StringEntity stringEntity = new StringEntity("{\n \"testName\": \"" + uuid.toString() + "\",\n \"id\": \"/db/Test/7766b85b-3b35-4258-b8da-48845cd0d92d\"\n}");
            httpPut.setEntity(stringEntity);
            httpClient.execute(httpPut);
            httpClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void deleteData() {

    }
}
