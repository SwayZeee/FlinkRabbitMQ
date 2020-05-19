package com.baqend.core;

import com.baqend.client.Client;
import com.baqend.client.baqend.BaqendClient;
import com.baqend.client.flink.FlinkClient;
import com.baqend.config.ConfigObject;
import com.baqend.messaging.RMQLatencySender;
import com.baqend.query.ExampleQuery;
import com.baqend.query.Query;
import com.google.gson.Gson;

import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;

public class QueryOrchestrator {

    private static RMQLatencySender rmqLatencySender;
    private static Query query;
    private static Client client;

    public static void main(String[] args) throws Exception {
        rmqLatencySender = new RMQLatencySender();
        query = new ExampleQuery();

        Gson gson = new Gson();
        //ConfigObject configObject = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\config\\config.json"), ConfigObject.class);
        ConfigObject configObject = gson.fromJson(new FileReader("C:\\Users\\Patrick\\Projects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\config\\config.json"), ConfigObject.class);
        System.out.println("Configuration file loaded");

        switch (configObject.clientToTest) {
            case 1:
                client = new BaqendClient(configObject);
                break;
            case 2:
                client = new FlinkClient(configObject);
                break;
            default:
                throw new Exception("Invalid configuration.");
        }

        registerQuery();
    }

    public static void registerQuery() throws IOException {
        UUID uuid = UUID.randomUUID();
        rmqLatencySender.sendMessage("tick" + "," + 1 + "," + uuid.toString() + "," + System.nanoTime());
        client.doQuery(query.getQuery());
    }
}
