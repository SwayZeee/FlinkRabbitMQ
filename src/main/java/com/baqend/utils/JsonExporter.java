package com.baqend.utils;

import com.baqend.core.measurement.Result;
import com.baqend.core.load.data.LoadData;
import com.baqend.core.load.workload.Workload;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class JsonExporter {
    private final Gson gson;

    public JsonExporter() {
        gson = new GsonBuilder().setPrettyPrinting().create();
    }

    public void exportLatenciesToJsonFile(Result result) {
        try {
            long timeStamp = System.currentTimeMillis();
            Writer writer = new FileWriter("src\\main\\java\\com\\baqend\\generated\\results\\" + timeStamp + ".json");
            gson.toJson(result, writer);
            writer.flush();
            writer.close();
            System.out.println("[JsonExporter] - Results exported (" + timeStamp + ".json)");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void exportInitialLoadToJsonFile(LoadData loadData) {
        try {
            Writer writer = new FileWriter("src\\main\\java\\com\\baqend\\generated\\load\\initialLoad.json");
            gson.toJson(loadData, writer);
            writer.flush();
            writer.close();
            System.out.println("[JsonExporter] - Initial load exported (initialLoad.json)");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void exportWorkloadToJsonFile(Workload workload) {
        long timeStamp = System.currentTimeMillis();
        try {
            Writer writer = new FileWriter("src\\main\\java\\com\\baqend\\generated\\workloads\\workload.json");
            gson.toJson(workload, writer);
            writer.flush();
            writer.close();
            System.out.println("[JsonExporter] - Workload exported (workload.json)");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
