package com.baqend.core;

import com.baqend.client.Client;
import com.baqend.config.ConfigObject;
import com.baqend.utils.JsonExporter;

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
        // TODO: random workload generation and saving of ids in a textfile
        // array of ids and data
        // pass to setup method
        client.setup();
    }

    public void warmUp() {
        System.out.println("Performing warm up");
        client.warmUp();
    }

    public void start() {
        System.out.println("Benchmark started");
        double x = 1;
        double rounds = 60;
        int throughput = 2000; // ops/s
        double startTime = System.currentTimeMillis();
        while (x <= rounds) {
            System.out.print("\rBenchmark in progess: " + (int) (x / rounds * 100) + "%");
            for (int i = 0; i < throughput; i++) {
                CompletableFuture.runAsync(this::step);
            }
            x++;
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        double stopTime = System.currentTimeMillis();
        double executionTime = stopTime - startTime;
        System.out.println();
        System.out.println("Benchmark done - Execution Time: " + executionTime + "ms");
        System.out.println("Throughput: " + throughput + " ops/s");
        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        JsonExporter jsonExporter = new JsonExporter();
        System.out.println("Quantitative Correctness: " + LatencyMeasurement.getInstance().getQuantitativeCorrectness());
        System.out.println("Average: " + LatencyMeasurement.getInstance().calculateAverage() + "ms");
        System.out.println("Median: " + LatencyMeasurement.getInstance().calculateMedian() + "ms");
        System.out.println("Maximum: " + LatencyMeasurement.getInstance().getMaximumLatency() + "ms");
        System.out.println("Minimum: " + LatencyMeasurement.getInstance().getMinimumLatency() + "ms");
        Map<UUID, Long> latencies = LatencyMeasurement.getInstance().calculateAllLatencies();
        try {
            jsonExporter.exportToJsonFile(latencies);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void step() {
        UUID transactionID = UUID.randomUUID();

        HashMap<String, String> values = new HashMap<>();
        values.put("testName", transactionID.toString());

        LatencyMeasurement.getInstance().tick(transactionID);
        client.insert("Test", transactionID.toString(), values, transactionID);
        //client.update("Test", "bbdc4d81-bbc0-4500-bec3-8d069b5ae2ad", values, transactionID);
        //client.delete("Test", "4eb13cec-b9b4-4ebb-98db-99c2d65c7ae5", transactionID);
    }

}
