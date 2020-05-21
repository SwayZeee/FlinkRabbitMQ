package com.baqend.utils;

import com.baqend.config.ConfigObject;

import java.util.HashMap;
import java.util.UUID;

public class ResultObject {
    private ConfigObject configObject;
    private int ticks;
    private int tocks;
    private double quantitativeCorrectness;
    private long avg;
    private long median;
    private long minimum;
    private long maximum;
    private long ninetiethPercentile;
    private long ninetyFifthPercentile;
    private long ninetyNinthPercentile;
    private HashMap<UUID, Long> measurements;

    public ResultObject(ConfigObject configObject,
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
                        HashMap<UUID, Long> measurements
    ) {
        this.configObject = configObject;
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
