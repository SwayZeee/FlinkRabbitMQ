package com.baqend.core;

import com.baqend.config.ConfigObject;
import com.baqend.utils.JsonExporter;
import com.baqend.utils.ResultObject;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class LatencyMeasurement {

    private final String EXCHANGE_NAME = "latencies";
    private Connection connection;
    private Channel channel;

    private static Map<UUID, Long> ticks = new ConcurrentHashMap<>();
    private static Map<UUID, Long> tocks = new ConcurrentHashMap<>();
    private UUID initialTick;

    private ConfigObject configObject;

    public LatencyMeasurement(ConfigObject configObject) throws IOException, TimeoutException {
        this.configObject = configObject;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println("[LatencyMeasurement] - Exchange: " + EXCHANGE_NAME + ", Queue: " + queueName);

        Consumer consumer = new Consumer() {
            @Override
            public void handleConsumeOk(String s) {

            }

            @Override
            public void handleCancelOk(String s) {

            }

            @Override
            public void handleCancel(String s) {

            }

            @Override
            public void handleShutdownSignal(String s, ShutdownSignalException e) {

            }

            @Override
            public void handleRecoverOk(String s) {

            }

            @Override
            public void handleDelivery(String s, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] bytes) {
                String message = new String(bytes);
                String[] tokens = message.split(",");
                if (tokens[0].equals("tick")) {
                    if (tokens[1].equals("1")) {
                        // is initial
                        setInitialTick(UUID.fromString(tokens[2]), Long.parseLong(tokens[3]));
                    } else {
                        tick(UUID.fromString(tokens[2]), Long.parseLong(tokens[3]));
                    }
                } else {
                    if (tokens[1].equals("1")) {
                        tock(Long.parseLong(tokens[3]));
                    } else {
                        tock(UUID.fromString(tokens[2]), Long.parseLong(tokens[3]));
                    }
                }
                //System.out.print("\r" + tocks.size() + "/" + ticks.size());
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }

    private void setInitialTick(UUID id, long timeStamp) {
        initialTick = id;
        ticks.put(id, timeStamp);
    }

    private void tick(UUID id, long timeStamp) {
        ticks.put(id, timeStamp);
    }

    private void tock(long timeStamp) {
        tocks.put(initialTick, timeStamp);
    }

    private void tock(UUID id, long timeStamp) {
        tocks.put(id, timeStamp);
    }

    private long calculateLatency(UUID uuid) {
        long start = ticks.get(uuid);
        long end = tocks.get(uuid);
        return end - start;
    }

    private HashMap<UUID, Long> calculateAllLatencies() {
        HashMap<UUID, Long> latencies = new HashMap<>();
        tocks.forEach((k, v) -> latencies.put(k, calculateLatency(k)));
        return latencies;
    }

    private HashMap<UUID, Long> sortLatencies(HashMap<UUID, Long> latencies) {
        return latencies.entrySet().stream()
                .sorted(comparingByValue()).collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    }

    private HashMap<Long, Long> indexLatencies(HashMap<UUID, Long> latencies) {
        AtomicLong index = new AtomicLong();
        index.set(1);
        HashMap<Long, Long> indexedLatencies = new HashMap<>();
        latencies.forEach((k, v) -> {
            indexedLatencies.put(index.get(), v);
            index.set(index.get() + 1);
        });
        return indexedLatencies;
    }

    private HashMap<Long, Long> sortAndIndexLatencies(HashMap<UUID, Long> latencies) {
        return indexLatencies(sortLatencies(latencies));
    }

    private long calculateAverage(HashMap<UUID, Long> latencies) {
        AtomicLong sum = new AtomicLong();
        latencies.forEach((k, v) -> sum.set(sum.get() + v));
        return sum.get() / latencies.size();
    }

    private long calculateMedian(HashMap<UUID, Long> latencies) {
        HashMap<Long, Long> indexedSortedLatencies = sortAndIndexLatencies(latencies);
        // returning the median
        long medianIndex;
        if (indexedSortedLatencies.size() % 2 == 1) {
            medianIndex = (latencies.size() + 1) / 2;
            return indexedSortedLatencies.get(medianIndex);
        }
        medianIndex = (latencies.size()) / 2;
        return (indexedSortedLatencies.get(medianIndex) + indexedSortedLatencies.get(medianIndex + 1)) / 2;
    }

    private long getMinimumLatency(HashMap<UUID, Long> latencies) {
        AtomicLong minimum = new AtomicLong();
        minimum.set(1000000000);
        latencies.forEach((k, v) -> {
            if (v < minimum.get()) {
                minimum.set(v);
            }
        });
        return minimum.get();
    }

    private long getMaximumLatency(HashMap<UUID, Long> latencies) {
        AtomicLong maximum = new AtomicLong();
        latencies.forEach((k, v) -> {
            if (v > maximum.get()) {
                maximum.set(v);
            }
        });
        return maximum.get();
    }

    private double getQuantitativeCorrectness() {
        return (double) tocks.size() / (double) ticks.size() * 100.0;
    }

    private Long calculateNthPercentile(HashMap<UUID, Long> latencies, double n) {
        HashMap<Long, Long> indexedSortedLatencies = sortAndIndexLatencies(latencies);
        return indexedSortedLatencies.get(Double.valueOf(Math.ceil(indexedSortedLatencies.size() * n)).longValue());
    }

    public void doCalculationsAndExport() {
        System.out.println("[LatencyMeasurement] - Performing Calculations and Export");
        System.out.println();

        System.out.println(ticks.size());
        System.out.println(tocks.size());

        HashMap<UUID, Long> latencies = calculateAllLatencies();
        double quantitativeCorrectness = getQuantitativeCorrectness();
        System.out.println("Quantitative Correctness: " + quantitativeCorrectness);
        long average = calculateAverage(latencies);
        System.out.println("Average: " + calculateAverage(latencies) + " ns");
        long median = calculateMedian(latencies);
        System.out.println("Median: " + median + " ns");
        long minimum = getMinimumLatency(latencies);
        System.out.println("Minimum: " + minimum + " ns");
        long maximum = getMaximumLatency(latencies);
        System.out.println("Maximum: " + maximum + " ns");
        long ninetiethPercentile = calculateNthPercentile(latencies, 0.9);
        System.out.println("90th Percentile: " + ninetiethPercentile + " ns");
        long ninetyFifthPercentile = calculateNthPercentile(latencies, 0.95);
        System.out.println("95th Percentile: " + ninetyFifthPercentile + " ns");
        long ninetyNinthPercentile = calculateNthPercentile(latencies, 0.99);
        System.out.println("99th Percentile: " + ninetyNinthPercentile + " ns");
        System.out.println();

        ResultObject resultObject = new ResultObject(configObject,
                ticks.size(),
                tocks.size(),
                quantitativeCorrectness,
                average,
                median,
                minimum,
                maximum,
                ninetiethPercentile,
                ninetyFifthPercentile,
                ninetyNinthPercentile,
                calculateAllLatencies()
        );
        JsonExporter jsonExporter = new JsonExporter();
        try {
            jsonExporter.exportLatenciesToJsonFile(resultObject);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("[LatencyMeasurement] - Calculations and Export done");
    }

    private void closeChannel() {
        try {
            channel.close();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    private void closeConnection() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        closeChannel();
        closeConnection();
        System.out.println("[LatencyMeasurement] - Stopped");
    }
}
