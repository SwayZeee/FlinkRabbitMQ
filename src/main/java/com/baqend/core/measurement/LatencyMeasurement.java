package com.baqend.core.measurement;

import com.baqend.clients.ClientChangeEvent;
import com.baqend.config.Config;
import com.baqend.utils.JsonExporter;
import com.google.gson.Gson;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class LatencyMeasurement implements Observer<ClientChangeEvent> {

    private static final Map<String, Long> ticks = new ConcurrentHashMap<>();
    private static final Map<String, Long> tocks = new ConcurrentHashMap<>();

    private Config config;

    public LatencyMeasurement(Config config) {
        this.config = config;
    }

    public void tick(String transactionID) {
        ticks.put(transactionID, System.nanoTime());
    }

    public void tock(String queryID, String transactionID) {
        tocks.put(queryID + "," + transactionID, System.nanoTime());
    }

    private long calculateLatency(String queryTransactionID) {
        String[] tokens = queryTransactionID.split(",");
        long start = ticks.get(tokens[1]);
        long end = tocks.get(queryTransactionID);
        return end - start;
    }

    private HashMap<String, Long> calculateAllLatencies() {
        HashMap<String, Long> latencies = new HashMap<>();
        tocks.forEach((k, v) -> latencies.put(k, calculateLatency(k)));
        return latencies;
    }

    private HashMap<String, Long> sortLatencies(HashMap<String, Long> latencies) {
        return latencies.entrySet().stream()
                .sorted(comparingByValue()).collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
    }

    private HashMap<Long, Long> indexLatencies(HashMap<String, Long> latencies) {
        AtomicLong index = new AtomicLong();
        index.set(1);
        HashMap<Long, Long> indexedLatencies = new HashMap<>();
        latencies.forEach((k, v) -> {
            indexedLatencies.put(index.get(), v);
            index.set(index.get() + 1);
        });
        return indexedLatencies;
    }

    private HashMap<Long, Long> sortAndIndexLatencies(HashMap<String, Long> latencies) {
        return indexLatencies(sortLatencies(latencies));
    }

    private long calculateAverage(HashMap<String, Long> latencies) {
        AtomicLong sum = new AtomicLong();
        latencies.forEach((k, v) -> sum.set(sum.get() + v));
        return sum.get() / latencies.size();
    }

    private long calculateMedian(HashMap<String, Long> latencies) {
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

    private long getMinimumLatency(HashMap<String, Long> latencies) {
        AtomicLong minimum = new AtomicLong();
        minimum.set(1000000000);
        latencies.forEach((k, v) -> {
            if (v < minimum.get()) {
                minimum.set(v);
            }
        });
        return minimum.get();
    }

    private long getMaximumLatency(HashMap<String, Long> latencies) {
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

    private Long calculateNthPercentile(HashMap<String, Long> latencies, double n) {
        HashMap<Long, Long> indexedSortedLatencies = sortAndIndexLatencies(latencies);
        return indexedSortedLatencies.get(Double.valueOf(Math.ceil(indexedSortedLatencies.size() * n)).longValue());
    }

    public void doCalculationsAndExport() throws FileNotFoundException {
        System.out.println("[LatencyMeasurement] - Performing Calculations and Export");
        System.out.println();

        System.out.println("Ticks: " + ticks.size());
        System.out.println("Tocks: " + tocks.size());

        HashMap<String, Long> latencies = calculateAllLatencies();
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

        Gson gson = new Gson();
        Config config = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\config\\config.json"), Config.class);

        MeasurementResult measurementResult = new MeasurementResult(config,
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
            jsonExporter.exportLatenciesToJsonFile(measurementResult, config.workload + "_" + config.throughput);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("[LatencyMeasurement] - Calculations and Export done");
    }

    @Override
    public void onSubscribe(@NonNull Disposable d) {

    }

    @Override
    public void onNext(ClientChangeEvent clientChangeEvent) {
        if (clientChangeEvent.getType().equals("result") && !config.isMeasuringInitialResult) {
            return;
        }
        tock(clientChangeEvent.getQueryID(), clientChangeEvent.getTransactionID());
    }

    @Override
    public void onError(@NonNull Throwable e) {

    }

    @Override
    public void onComplete() {
        try {
            doCalculationsAndExport();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
