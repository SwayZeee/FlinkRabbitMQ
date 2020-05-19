package com.baqend.core;

import com.baqend.utils.JsonExporter;
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

    private static final String EXCHANGE_NAME = "latencies";
    private static Connection connection;
    private static Channel channel;

    private static final Map<UUID, Long> ticks = new ConcurrentHashMap<>();
    private static final Map<UUID, Long> tocks = new ConcurrentHashMap<>();
    private static UUID initialTick;

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println("Exchange: " + EXCHANGE_NAME + " Queue: " + queueName);

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
                System.out.print("\r" + tocks.size() + "/" + ticks.size());
            }
        };
        channel.basicConsume(queueName, true, consumer);
        try {
            System.out.println("Press Enter to do calculations and export ...");
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        doCalculationsAndExport();
    }

    public static void setInitialTick(UUID id, long timeStamp) {
        initialTick = id;
        ticks.put(id, timeStamp);
    }

    public static void tick(UUID id, long timeStamp) {
        ticks.put(id, timeStamp);
    }

    public static void tock(long timeStamp) {
        tocks.put(initialTick, timeStamp);
    }

    public static void tock(UUID id, long timeStamp) {
        tocks.put(id, timeStamp);
    }

    public static long calculateLatency(UUID uuid) {
        long start = ticks.get(uuid);
        long end = tocks.get(uuid);
        return end - start;
    }

    public static HashMap<UUID, Long> calculateAllLatencies() {
        HashMap<UUID, Long> latencies = new HashMap<>();
        tocks.forEach((k, v) -> latencies.put(k, calculateLatency(k)));
        return latencies;
    }

    public static long calculateAverage() {
        AtomicLong sum = new AtomicLong();
        tocks.forEach((k, v) -> sum.set(sum.get() + calculateLatency(k)));
        return sum.get() / ticks.size();
    }

    public static Long calculateMedian() {
        AtomicLong index = new AtomicLong();
        index.set(0);
        // calculating all latencies
        Map<UUID, Long> latencies = calculateAllLatencies();
        // sorting the calculated latencies
        Map<UUID, Long> sortedLatencies =
                latencies.entrySet().stream()
                        .sorted(comparingByValue()).collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
        // map sorted latencies to an indexed map
        Map<Long, Long> indexedSortedLatencies = new HashMap<>();
        sortedLatencies.forEach((k, v) -> {
            indexedSortedLatencies.put(index.get(), calculateLatency(k));
            index.set(index.get() + 1);
        });
        // returning the median
        long medianIndex;
        if (indexedSortedLatencies.size() % 2 == 1) {
            medianIndex = (latencies.size() - 1) / 2;
            return indexedSortedLatencies.get(medianIndex);
        }
        medianIndex = (latencies.size()) / 2;
        return (indexedSortedLatencies.get(medianIndex - 1) + indexedSortedLatencies.get(medianIndex)) / 2;
    }

    public static Long getMaximumLatency() {
        Map<UUID, Long> latencies = calculateAllLatencies();
        AtomicLong maximum = new AtomicLong();
        latencies.forEach((k, v) -> {
            if (v > maximum.get()) {
                maximum.set(v);
            }
        });
        return maximum.get();
    }

    public static Long getMinimumLatency() {
        Map<UUID, Long> latencies = calculateAllLatencies();
        AtomicLong minimum = new AtomicLong();
        minimum.set(1000000000);
        latencies.forEach((k, v) -> {
            if (v < minimum.get()) {
                minimum.set(v);
            }
        });
        return minimum.get();
    }

    public static String getQuantitativeCorrectness() {
        double correctness = (double) tocks.size() / (double) ticks.size() * 100.0;
        return tocks.size() + "/" + ticks.size() + " (" + correctness + " %)";
    }

    public static Long calculateNthPercentile(double n) {
        AtomicLong index = new AtomicLong();
        index.set(0);
        // calculating all latencies
        Map<UUID, Long> latencies = new HashMap<>();
        tocks.forEach((k, v) -> latencies.put(k, calculateLatency(k)));
        // sorting the calculated latencies
        Map<UUID, Long> sortedLatencies =
                latencies.entrySet().stream()
                        .sorted(comparingByValue()).collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
        // map sorted latencies to an indexed map
        Map<Long, Long> indexedSortedLatencies = new HashMap<>();
        sortedLatencies.forEach((k, v) -> {
            indexedSortedLatencies.put(index.get(), calculateLatency(k));
            index.set(index.get() + 1);
        });
        return indexedSortedLatencies.get(Double.valueOf(Math.ceil(indexedSortedLatencies.size() * n)).longValue());
    }

    public static void doCalculationsAndExport() {
        System.out.println("Quantitative Correctness: " + getQuantitativeCorrectness());
        System.out.println("Average: " + calculateAverage() + " ns");
        System.out.println("Median: " + calculateMedian() + " ns");
        System.out.println("Maximum: " + getMaximumLatency() + " ns");
        System.out.println("Minimum: " + getMinimumLatency() + " ns");
        System.out.println("90th Percentile: " + calculateNthPercentile(0.9) + " ns");
        System.out.println("95th Percentile: " + calculateNthPercentile(0.95) + " ns");
        System.out.println("99th Percentile: " + calculateNthPercentile(0.99) + " ns");
        HashMap<UUID, Long> latencies = calculateAllLatencies();
        JsonExporter jsonExporter = new JsonExporter();
        try {
            jsonExporter.exportLatenciesToJsonFile(latencies);
        } catch (Exception e) {
            e.printStackTrace();
        }
        close();
    }

    public static void closeChannel() {
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public static void closeConnection() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        closeChannel();
        closeConnection();
    }

}
