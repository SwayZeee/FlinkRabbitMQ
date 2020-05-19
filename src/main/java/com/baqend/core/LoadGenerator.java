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
        RateLimiter rateLimiter = RateLimiter.create(configObject.throughput);
        double x = 1;
        int rounds = configObject.duration;
        int throughput = configObject.throughput;

        System.out.println("Benchmark started - Throughput: " + throughput + " ops/s");
        double startTime = System.currentTimeMillis();
        while (x <= rounds * throughput) {
            System.out.print("\rBenchmark in progess: " + (int) (x / (rounds * throughput) * 100) + " %");
            rateLimiter.acquire();
            step((int) x);
            x++;
        }

        double stopTime = System.currentTimeMillis();
        double executionTime = stopTime - startTime;
        System.out.println();
        System.out.println("Benchmark done - Execution Time: " + executionTime + " ms");
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
            try {
                rmqLatencySender.sendMessage("tick" + "," + 0 + "," + transactionID.toString() + "," + System.nanoTime());
            } catch (IOException e) {
                e.printStackTrace();
            }
            client.insert("Test", transactionID.toString(), randomDataGenerator.generateRandomDataset(1), transactionID);
            //client.update("Test", "7e1df1e5-c9fb-457a-9efe-48a543a4dd2e", randomDataGenerator.generateRandomDataset(1), transactionID);
            return;
        }
        client.insert("Test", transactionID.toString(), randomDataGenerator.generateRandomDataset(0), transactionID);
        //client.update("Test", "7e1df1e5-c9fb-457a-9efe-48a543a4dd2e", randomDataGenerator.generateRandomDataset(0), transactionID);
        //client.delete("Test", "7e1df1e5-c9fb-457a-9efe-48a543a4dd2e", transactionID);
    }

    public void stop() {
        client.cleanUp("Test");
        rmqLatencySender.close();
    }


}
