package com.baqend.utils;

import com.baqend.core.measurement.MeasurementResult;
import com.baqend.core.subscription.query.QuerySet;
import com.baqend.workload.Workload;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;

public class JsonExporter {
    private final Gson gson;

    public JsonExporter() {
        gson = new GsonBuilder().setPrettyPrinting().create();
    }

    public void exportLatenciesToJsonFile(MeasurementResult measurementResult, ArrayList<Long> latencies, String directoryName) {
        try {
            long timeStamp = System.currentTimeMillis();
            new File("src\\main\\java\\com\\baqend\\generated\\results\\" + directoryName).mkdirs();
            Writer writer1 = new FileWriter("src\\main\\java\\com\\baqend\\generated\\results\\" + directoryName + "\\" + timeStamp + "_result.json");
            gson.toJson(measurementResult, writer1);
            writer1.flush();
            writer1.close();
            Writer writer2 = new FileWriter("src\\main\\java\\com\\baqend\\generated\\results\\" + directoryName + "\\" + timeStamp + "_latencies.json");
            gson.toJson(latencies, writer2);
            writer2.flush();
            writer2.close();
            System.out.println("[JsonExporter] - Results exported (" + timeStamp + "_result.json)");
            System.out.println("[JsonExporter] - Latencies exported (" + timeStamp + "_latencies.json)");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void exportInitialLoadToJsonFile(Workload workload, String fileName) {
        try {
            new File("src\\main\\java\\com\\baqend\\generated\\workloads\\").mkdirs();
            Writer writer = new FileWriter("src\\main\\java\\com\\baqend\\generated\\workloads\\" + fileName + ".json");
            gson.toJson(workload, writer);
            writer.flush();
            writer.close();
            System.out.println("[JsonExporter] - Workload exported (" + fileName + ".json)");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void exportWorkloadToJsonFile(Workload workload, String fileName, int throughput) {
        try {
            new File("src\\main\\java\\com\\baqend\\generated\\workloads\\" + fileName).mkdirs();
            Writer writer = new FileWriter("src\\main\\java\\com\\baqend\\generated\\workloads\\" + fileName + "\\" + fileName + "_" + throughput + ".json");
            gson.toJson(workload, writer);
            writer.flush();
            writer.close();
            System.out.println("[JsonExporter] - Workload exported (" + fileName + ".json)");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void exportQuerySetToJsonFile(QuerySet querySet, String fileName) {
        try {
            new File("src\\main\\java\\com\\baqend\\generated\\querysets").mkdirs();
            Writer writer = new FileWriter("src\\main\\java\\com\\baqend\\generated\\querysets\\" + fileName + ".json");
            gson.toJson(querySet, writer);
            writer.flush();
            writer.close();
            System.out.println("[JsonExporter] - QuerySet exported (" + fileName + ".json)");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
