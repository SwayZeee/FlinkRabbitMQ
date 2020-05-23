package com.baqend.core.load;

import com.baqend.clients.Client;
import com.baqend.config.Config;
import com.baqend.core.load.data.LoadData;
import com.baqend.core.load.workload.Workload;
import com.baqend.core.load.workload.WorkloadEvent;
import com.baqend.core.load.workload.WorkloadEventType;
import com.baqend.messaging.RMQLatencySender;
import com.baqend.utils.RandomDataGenerator;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class LoadGenerator {

    private final Gson gson = new Gson();
    private final Client client;
    private final Config config;
    private final RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
    private final RMQLatencySender rmqLatencySender = new RMQLatencySender();

    public LoadGenerator(Client client, Config config) throws IOException, TimeoutException {
        this.client = client;
        this.config = config;
    }

    public void load() throws FileNotFoundException {
        System.out.println("[LoadGenerator] - Performing Load");
        double startTime = System.currentTimeMillis();
        LoadData loadData = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\generated\\load\\initialLoad.json"), LoadData.class);
        RateLimiter rateLimiter = RateLimiter.create(500);
        double x = 1;
        while (x <= loadData.getLoad().size()) {
            System.out.print("\r[LoadGenerator] - Load in progess " + (int) (x / (loadData.getLoad().size()) * 100) + " %");
            rateLimiter.acquire();
            client.insert("Test", loadData.getLoad().get((int) x - 1).getUuid().toString(), loadData.getLoad().get((int) x - 1).getData(), UUID.randomUUID());
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
        RateLimiter rateLimiter = RateLimiter.create(config.throughput);
        double x = 1;
        int rounds = config.duration;
        int throughput = config.throughput;

        Workload workload = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\generated\\workloads\\workload.json"), Workload.class);

        System.out.println("[LoadGenerator] - Performing Benchmark (" + throughput + " ops/s)");
        double startTime = System.currentTimeMillis();
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
            rmqLatencySender.sendMessage("tick" + "," + 0 + "," + workloadEvent.getTransactionID() + "," + System.nanoTime());
        }
        if (workloadEvent.getType() == WorkloadEventType.INSERT) {
            client.insert("Test", workloadEvent.getSingleDataSet().getUuid().toString(), workloadEvent.getSingleDataSet().getData(), workloadEvent.getTransactionID());
        } else if (workloadEvent.getType() == WorkloadEventType.UPDATE) {
            client.update("Test", workloadEvent.getSingleDataSet().getUuid().toString(), workloadEvent.getSingleDataSet().getData(), workloadEvent.getTransactionID());
        } else {
            client.delete("Test", workloadEvent.getSingleDataSet().getUuid().toString(), workloadEvent.getTransactionID());
        }

//        UUID transactionID = UUID.randomUUID();
//        if (count % (configObject.throughput / 100) == 0) {
//            rmqLatencySender.sendMessage("tick" + "," + 0 + "," + transactionID.toString() + "," + System.nanoTime());
//            client.insert("Test", transactionID.toString(), randomDataGenerator.generateRandomDataset(1), transactionID);
//            //client.update("Test", "7e1df1e5-c9fb-457a-9efe-48a543a4dd2e", randomDataGenerator.generateRandomDataset(1), transactionID);
//            return;
//        }
//        client.insert("Test", transactionID.toString(), randomDataGenerator.generateRandomDataset(0), transactionID);
//        //client.update("Test", "7e1df1e5-c9fb-457a-9efe-48a543a4dd2e", randomDataGenerator.generateRandomDataset(0), transactionID);
    }

    public void cleanUp() {
        System.out.println("[LoadGenerator] - Performing CleanUp");
        client.cleanUp("Test");
        System.out.println("[LoadGenerator] - CleanUp done");
    }

    public void stop() {
        client.close();
        rmqLatencySender.close();
        System.out.println("[LoadGenerator] - Stopped");
    }
}
