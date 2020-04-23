package com.baqend;

import com.baqend.client.Client;
import com.baqend.utils.JsonExporter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class LoadGenerator {

    private Client client;
    private ConfigObject configObject;
    private JsonExporter jsonExporter;

    public LoadGenerator(Client client, ConfigObject configObject) {
        this.client = client;
        this.configObject = configObject;
        this.jsonExporter = new JsonExporter();
    }

    public void setup() throws Exception {
        System.out.println("Performing setup");
        client.setup();
    }

    public void warmUp() {
        System.out.println("Performing warm up");
        client.warmUp();
    }

    public void start() throws InterruptedException, IOException, TimeoutException {
        long startTime = System.currentTimeMillis();
        System.out.println("Benchmark started");
        double x = 1;
        int rounds = 100;
        while (x <= rounds) {
            System.out.print("\rBenchmark in progess: " + (int) (x / rounds * 100) + "%");
            step();
            x++;
            Thread.sleep(1000);
        }
        long stopTime = System.currentTimeMillis();
        long executionTime = stopTime - startTime;
        System.out.println();
        System.out.println("Benchmark done - Execution Time: " + executionTime + "ms");
        double throughput = rounds / (executionTime / 1000);
        System.out.println("Throughput: " + throughput + " ops/s");
        Thread.sleep(1000);
    }

    public void stop() throws IOException {
        Map<UUID, Long> latencies = LatencyMeasurement.getInstance().calculateAllLatencies();
        //latencies.forEach((k, v) -> System.out.println(k + " : " + v + "ms"));

        System.out.println("Quantitative Correctness: " + LatencyMeasurement.getInstance().getQuantitativeCorrectness());
        System.out.println("Average: " + LatencyMeasurement.getInstance().calculateAverage() + "ms");
        System.out.println("Median: " + LatencyMeasurement.getInstance().calculateMedian() + "ms");
        System.out.println("Maximum: " + LatencyMeasurement.getInstance().getMaximumLatency() + "ms");
        System.out.println("Minimum: " + LatencyMeasurement.getInstance().getMinimumLatency() + "ms");

        jsonExporter.exportToJsonFile(latencies);
    }

    public void step() throws IOException, TimeoutException {
        UUID uuid = UUID.randomUUID();
        LatencyMeasurement.getInstance().tick(uuid);
        client.updateData(uuid);
    }
}
