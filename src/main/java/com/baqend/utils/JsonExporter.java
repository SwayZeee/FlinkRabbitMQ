package com.baqend.utils;

import com.baqend.workload.LoadData;
import com.baqend.workload.Workload;
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

    public void exportLatenciesToJsonFile(ResultObject resultObject) {
        try {
            long timeStamp = System.currentTimeMillis();
            Writer writer = new FileWriter("C:\\Users\\Patrick\\Projects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\results\\" + timeStamp + ".json");
            gson.toJson(resultObject, writer);
            writer.flush();
            writer.close();
            System.out.println("[JsonExporter] - Results exported (" + timeStamp + ".json)");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void exportInitialLoadToJsonFile(LoadData loadData) {
        try {
            Writer writer = new FileWriter("C:\\Users\\Patrick\\Projects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\workload\\initialLoad.json");
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
            Writer writer = new FileWriter("C:\\Users\\Patrick\\Projects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\workload\\workload.json");
            gson.toJson(workload, writer);
            writer.flush();
            writer.close();
            System.out.println("[JsonExporter] - Workload exported (workload.json)");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
