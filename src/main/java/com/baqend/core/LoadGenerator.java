package com.baqend.core;

import com.baqend.client.Client;
import com.baqend.config.ConfigObject;
import com.baqend.messaging.RMQLatencySender;
import com.baqend.utils.RandomDataGenerator;
import com.baqend.workload.LoadData;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class LoadGenerator {

    private final Client client;
    private final ConfigObject configObject;
    private final RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
    private final RMQLatencySender rmqLatencySender = new RMQLatencySender();

    public LoadGenerator(Client client, ConfigObject configObject) throws IOException, TimeoutException {
        this.client = client;
        this.configObject = configObject;
    }

    public void load() throws FileNotFoundException {
        System.out.println("[LoadGenerator] - Performing Load");
        Gson gson = new Gson();
        LoadData loadData = gson.fromJson(new FileReader("C:\\Users\\Patrick\\Projects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\workload\\initialLoad.json"), LoadData.class);
        System.out.println("[LoadGenerator] - Load done");
    }

    public void warmUp() {
        System.out.println("[LoadGenerator] - Performing WarmUp");
        //
        System.out.println("[LoadGenerator] - WarmUp done");
    }

    public void start() {
        RateLimiter rateLimiter = RateLimiter.create(configObject.throughput);
        double x = 1;
        int rounds = configObject.duration;
        int throughput = configObject.throughput;

        System.out.println("[LoadGenerator] - Performing Benchmark (" + throughput + " ops/s)");
        double startTime = System.currentTimeMillis();
        while (x <= rounds * throughput) {
            System.out.print("\r[LoadGenerator] - Benchmark in progess " + (int) (x / (rounds * throughput) * 100) + " %");
            rateLimiter.acquire();
            step((int) x);
            x++;
        }
        double stopTime = System.currentTimeMillis();
        double executionTime = stopTime - startTime;
        System.out.println("\r[LoadGenerator] - Benchmark done (" + executionTime + " ms)");
        try {
            Thread.sleep(configObject.waitingTime);
        } catch (
                Exception e) {
            e.printStackTrace();
        }
    }

    public void step(int count) {
        UUID transactionID = UUID.randomUUID();
        if (count % (configObject.throughput / 100) == 0) {
            rmqLatencySender.sendMessage("tick" + "," + 0 + "," + transactionID.toString() + "," + System.nanoTime());
            client.insert("Test", transactionID.toString(), randomDataGenerator.generateRandomDataset(1), transactionID);
            //client.update("Test", "7e1df1e5-c9fb-457a-9efe-48a543a4dd2e", randomDataGenerator.generateRandomDataset(1), transactionID);
            return;
        }
        client.insert("Test", transactionID.toString(), randomDataGenerator.generateRandomDataset(0), transactionID);
        //client.update("Test", "7e1df1e5-c9fb-457a-9efe-48a543a4dd2e", randomDataGenerator.generateRandomDataset(0), transactionID);
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
