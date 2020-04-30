package com.baqend.core;

import com.baqend.client.baqend.BaqendClient;
import com.baqend.client.Client;
import com.baqend.client.flink.FlinkClient;
import com.baqend.config.ConfigObject;
import com.baqend.query.ExampleQuery;
import com.baqend.query.Query;
import com.baqend.utils.JsonExporter;
import com.google.gson.Gson;

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class Benchmark {
    public static void main(String[] args) throws Exception {
        System.out.println("+++ Benchmark started +++");

        Gson gson = new Gson();
        ConfigObject configObject = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\config\\config.json"), ConfigObject.class);
        System.out.println("Configuration file loaded");

        Client client;
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

        Query query = new ExampleQuery();

        QueryOrchestrator queryOrchestrator = new QueryOrchestrator(query, client);
        queryOrchestrator.registerQuery();

        LoadGenerator loadGenerator = new LoadGenerator(client, configObject);
        loadGenerator.setup();
        loadGenerator.warmUp();
        loadGenerator.start();
        loadGenerator.stop();
    }
}
