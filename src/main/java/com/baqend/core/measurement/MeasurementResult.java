package com.baqend.core.measurement;

import com.baqend.config.Config;

import java.util.HashMap;

public class MeasurementResult {
    private final Config config;
    private final int ticks;
    private final int tocks;
    private final double quantitativeCorrectness;
    private final long avg;
    private final long median;
    private final long minimum;
    private final long maximum;
    private final long ninetiethPercentile;
    private final long ninetyFifthPercentile;
    private final long ninetyNinthPercentile;
    private final HashMap<String, Long> measurements;

    public MeasurementResult(Config config,
                             int ticks,
                             int tocks,
                             double quantitativeCorrectness,
                             long avg,
                             long median,
                             long minimum,
                             long maximum,
                             long ninetiethPercentile,
                             long ninetyFifthPercentile,
                             long ninetyNinthPercentile,
                             HashMap<String, Long> measurements
    ) {
        this.config = config;
        this.ticks = ticks;
        this.tocks = tocks;
        this.quantitativeCorrectness = quantitativeCorrectness;
        this.avg = avg;
        this.median = median;
        this.minimum = minimum;
        this.maximum = maximum;
        this.ninetiethPercentile = ninetiethPercentile;
        this.ninetyFifthPercentile = ninetyFifthPercentile;
        this.ninetyNinthPercentile = ninetyNinthPercentile;
        this.measurements = measurements;
    }
}
