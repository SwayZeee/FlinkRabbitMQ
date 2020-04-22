package com.baqend;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class LatencyMeasurement {

    private static LatencyMeasurement single_instance = null;
    private Map<UUID, Long> ticks = new HashMap();
    private Map<UUID, Long> tocks = new HashMap();
    private UUID initialTick;

    private LatencyMeasurement() {
    }

    public static LatencyMeasurement getInstance() {
        if (single_instance == null) {
            single_instance = new LatencyMeasurement();
        }
        return single_instance;
    }

    public void setInitialTick(UUID uuid) {
        this.initialTick = uuid;
    }

    public void tick(UUID id) {
        ticks.put(id, System.currentTimeMillis());
    }

    public void tock() {
        tocks.put(initialTick, System.currentTimeMillis());
    }

    public void tock(UUID id) {
        tocks.put(id, System.currentTimeMillis());
    }

    public long calculateLatency(UUID uuid) {
        long start = ticks.get(uuid);
        long end = tocks.get(uuid);
        return end - start;
    }

    public Map<UUID, Long> calculateAllLatencies() {
        Map<UUID, Long> latencies = new HashMap();
        ticks.forEach((k, v) -> latencies.put(k, calculateLatency(k)));
        return latencies;
    }

    public long calculateAverage() {
        AtomicLong sum = new AtomicLong();
        ticks.forEach((k, v) -> sum.set(sum.get() + calculateLatency(k)));
        return sum.get() / ticks.size();
    }

    public Long calculateMedian() {
        AtomicLong index = new AtomicLong();
        index.set(0);
        // calculating all latencies
        Map<UUID, Long> latencies = new HashMap();
        ticks.forEach((k, v) -> latencies.put(k, calculateLatency(k)));
        // sorting the calculated latencies
        Map<UUID, Long> sortedLatencies =
                latencies.entrySet().stream()
                        .sorted(comparingByValue()).collect(toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2, LinkedHashMap::new));
        // map sorted latencies to an indexed map
        Map<Long, Long> indexedSortedLatencies = new HashMap();
        sortedLatencies.forEach((k, v) -> {
            indexedSortedLatencies.put(index.get(), calculateLatency(k));
            index.set(index.get() + 1);
        });
        // returning the median
        long medianIndex = 0;
        if (indexedSortedLatencies.size() % 2 == 1) {
            medianIndex = (latencies.size() - 1) / 2;
            return indexedSortedLatencies.get(medianIndex);
        }
        medianIndex = (latencies.size()) / 2;
        return (indexedSortedLatencies.get(medianIndex - 1) + indexedSortedLatencies.get(medianIndex)) / 2;
    }

    public Long getMaximumLatency() {
        Map<UUID, Long> latencies = calculateAllLatencies();
        AtomicLong maximum = new AtomicLong();
        latencies.forEach((k, v) -> {if(v > maximum.get()){
            maximum.set(v);
        }});
        return maximum.get();
    }

    public Long getMinimumLatency() {
        Map<UUID, Long> latencies = calculateAllLatencies();
        AtomicLong minimum = new AtomicLong();
        minimum.set(1000000);
        latencies.forEach((k, v) -> {if(v < minimum.get()){
            minimum.set(v);
        }});
        return minimum.get();
    }
}
