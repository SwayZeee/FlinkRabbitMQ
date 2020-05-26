package com.baqend.core.load;

import com.baqend.clients.Client;
import com.baqend.config.Config;
import com.baqend.workload.Workload;
import com.baqend.workload.WorkloadEvent;
import com.baqend.workload.WorkloadEventType;
import com.baqend.core.measurement.LatencyMeasurement;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.UUID;

public class LoadGenerator {

    private final Gson gson = new Gson();
    private final Client client;
    private final Config config;
    private final LatencyMeasurement latencyMeasurement;

    public LoadGenerator(Client client, Config config, LatencyMeasurement latencyMeasurement) {
        this.client = client;
        this.config = config;
        this.latencyMeasurement = latencyMeasurement;
    }

    public void load() throws FileNotFoundException {
        final int INSERT_RATE = 2500;
        System.out.println("[LoadGenerator] - Performing Load (" + config.initialLoadFile + ".json @ " + INSERT_RATE + " ops/s)");
        Workload initialWorkloadData = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\generated\\workloads\\" + config.initialLoadFile + ".json"), Workload.class);
        double startTime = System.currentTimeMillis();
        double x = 1;
        RateLimiter rateLimiter = RateLimiter.create(INSERT_RATE);
        while (x <= initialWorkloadData.getWorkload().size()) {
            System.out.print("\r[LoadGenerator] - Load in progess " + (int) (x / (initialWorkloadData.getWorkload().size()) * 100) + " %");
            rateLimiter.acquire();
            client.insert("Test", initialWorkloadData.getWorkload().get((int) x - 1).getSingleDataSet().getUuid().toString(), initialWorkloadData.getWorkload().get((int) x - 1).getSingleDataSet().getData(), UUID.randomUUID());
            x++;
        }
        double stopTime = System.currentTimeMillis();
        double executionTime = stopTime - startTime;
        System.out.println("\r[LoadGenerator] - Load done (" + executionTime + " ms)");
        try {
            Thread.sleep(config.waitingTime);
        } catch (
                Exception e) {
            e.printStackTrace();
        }
    }

    public void warmUp() {
        System.out.println("[LoadGenerator] - Performing WarmUp");
        //
        System.out.println("[LoadGenerator] - WarmUp done");
    }

    public void start() throws FileNotFoundException {
        double x = 1;
        int rounds = config.duration;
        int throughput = config.throughput;
        Workload workload = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\generated\\workloads\\" + config.workload + "\\" + config.workload + "_" + config.throughput + ".json"), Workload.class);
        System.out.println("[LoadGenerator] - Performing Benchmark (" + config.workload + "_" + config.throughput + ".json @ " + throughput + " ops/s)");
        double startTime = System.currentTimeMillis();
        RateLimiter rateLimiter = RateLimiter.create(config.throughput);
        while (x <= rounds * throughput) {
            System.out.print("\r[LoadGenerator] - Benchmark in progess " + (int) (x / (rounds * throughput) * 100) + " %");
            rateLimiter.acquire();
            step(workload.getWorkload().get((int) x - 1));
            x++;
        }
        double stopTime = System.currentTimeMillis();
        double executionTime = stopTime - startTime;
        System.out.println("\r[LoadGenerator] - Benchmark done (" + executionTime + " ms)");
        try {
            Thread.sleep(config.waitingTime);
        } catch (
                Exception e) {
            e.printStackTrace();
        }
    }

    public void step(WorkloadEvent workloadEvent) {
        if (workloadEvent.isRelevant()) {
            latencyMeasurement.tick(workloadEvent.getTransactionID().toString());
        }
        if (workloadEvent.getType() == WorkloadEventType.INSERT) {
            client.insert("Test", workloadEvent.getSingleDataSet().getUuid().toString(), workloadEvent.getSingleDataSet().getData(), workloadEvent.getTransactionID());
        } else if (workloadEvent.getType() == WorkloadEventType.UPDATE) {
            client.update("Test", workloadEvent.getSingleDataSet().getUuid().toString(), workloadEvent.getSingleDataSet().getData(), workloadEvent.getTransactionID());
        } else {
            client.delete("Test", workloadEvent.getSingleDataSet().getUuid().toString(), workloadEvent.getTransactionID());
        }
    }

    public void cleanUp() {
        System.out.println("[LoadGenerator] - Performing CleanUp");
        client.cleanUp("Test");
        System.out.println("[LoadGenerator] - CleanUp done");
    }

    public void stop() {
        client.close();
        System.out.println("[LoadGenerator] - Stopped");
    }
}
