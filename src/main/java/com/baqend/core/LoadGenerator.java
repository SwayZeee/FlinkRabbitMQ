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
        double x = 1;
        double rounds = 60;
        int throughput = 2500; // ops/s

        System.out.println("Benchmark started - Throughput: " + throughput + " ops/s");
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
        try {
            Thread.sleep(15000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        System.out.println("Quantitative Correctness: " + LatencyMeasurement.getInstance().getQuantitativeCorrectness());
        System.out.println("Average: " + LatencyMeasurement.getInstance().calculateAverage() + "ms");
        System.out.println("Median: " + LatencyMeasurement.getInstance().calculateMedian() + "ms");
        System.out.println("Maximum: " + LatencyMeasurement.getInstance().getMaximumLatency() + "ms");
        System.out.println("Minimum: " + LatencyMeasurement.getInstance().getMinimumLatency() + "ms");
        System.out.println("90th Percentile: " + LatencyMeasurement.getInstance().calculateNthPercentile(0.9) + "ms");
        System.out.println("95th Percentile: " + LatencyMeasurement.getInstance().calculateNthPercentile(0.95) + "ms");
        System.out.println("99th Percentile: " + LatencyMeasurement.getInstance().calculateNthPercentile(0.99) + "ms");
        Map<UUID, Long> latencies = LatencyMeasurement.getInstance().calculateAllLatencies();
        JsonExporter jsonExporter = new JsonExporter();
        try {
            jsonExporter.exportToJsonFile(latencies);
        } catch (Exception e) {
            e.printStackTrace();
        }
        client.cleanUp();
    }

    public void step() {
        UUID transactionID = UUID.randomUUID();

        HashMap<String, String> values = new HashMap<>();
        values.put("testName", transactionID.toString());

        LatencyMeasurement.getInstance().tick(transactionID);
        client.insert("Test", transactionID.toString(), values, transactionID);
        //client.update("Test", "7e1df1e5-c9fb-457a-9efe-48a543a4dd2e", values, transactionID);
        //client.delete("Test", "7e1df1e5-c9fb-457a-9efe-48a543a4dd2e", transactionID);
    }

}
