package com.baqend.core;

import com.baqend.client.Client;
import com.baqend.config.ConfigObject;
import com.baqend.utils.JsonExporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class LoadGenerator {

    private final Client client;

    public LoadGenerator(Client client, ConfigObject configObject) {
        this.client = client;
    }

    public void setup() {
        System.out.println("Performing setup");
        client.setup();
    }

    public void warmUp() {
        System.out.println("Performing warm up");
        client.warmUp();
    }

    public void start() {
        System.out.println("Benchmark started");
        double x = 1;
        double rounds = 10;
        while (x <= rounds) {
            System.out.print("\rBenchmark in progess: " + (int) (x / rounds * 100) + "%");
            CompletableFuture.runAsync(this::step);
            x++;
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void step() {
        UUID transactionID = UUID.randomUUID();

        HashMap<String, String> values = new HashMap<>();
        values.put("testName", transactionID.toString());

        LatencyMeasurement.getInstance().tick(transactionID);
        client.insert("Test", transactionID.toString(), values, transactionID);
        //client.update("Test", "f822e35d-03f2-433b-a361-3e0794b72582", values, transactionID);
        //client.delete("Test", "4eb13cec-b9b4-4ebb-98db-99c2d65c7ae5", transactionID);
    }
}
