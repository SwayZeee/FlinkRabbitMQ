package com.baqend.core;

import com.baqend.client.Client;
import com.baqend.config.ConfigObject;
import com.baqend.utils.JsonExporter;
import com.baqend.utils.RandomDataGenerator;
import com.baqend.workload.LoadData;
import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class LoadGenerator {

    private final Client client;
    private final RandomDataGenerator randomDataGenerator = new RandomDataGenerator();

    public LoadGenerator(Client client, ConfigObject configObject) {
        this.client = client;
    }

    public void setup() throws FileNotFoundException {
        System.out.println("Performing setup");
        double startTime = System.currentTimeMillis();
        Gson gson = new Gson();
        LoadData loadData = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\workload\\initialLoad.json"), LoadData.class);
        client.setup("Test", loadData);
        double stopTime = System.currentTimeMillis();
        double executionTime = stopTime - startTime;
        System.out.print("Setup completed (" + executionTime + "ms) - loaded " + loadData.getLoad().size() + " datasets to database");
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

    public void step() {
        UUID transactionID = UUID.randomUUID();
        LatencyMeasurement.getInstance().tick(transactionID);
        client.insert("Test", transactionID.toString(), randomDataGenerator.generateRandomDataset(), transactionID);
        //client.update("Test", "7e1df1e5-c9fb-457a-9efe-48a543a4dd2e", randomDataGenerator.generateRandomDataset(), transactionID);
        //client.delete("Test", "7e1df1e5-c9fb-457a-9efe-48a543a4dd2e", transactionID);
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
        HashMap<UUID, Long> latencies = LatencyMeasurement.getInstance().calculateAllLatencies();
        JsonExporter jsonExporter = new JsonExporter();
        try {
            jsonExporter.exportLatenciesToJsonFile(latencies);
        } catch (Exception e) {
            e.printStackTrace();
        }
        client.cleanUp("Test");
    }
}
