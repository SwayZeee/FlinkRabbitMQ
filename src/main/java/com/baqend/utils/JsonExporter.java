package com.baqend.utils;

import com.baqend.workload.LoadData;
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

    public void exportLatenciesToJsonFile(ResultObject resultObject) throws IOException {
        long timeStamp = System.currentTimeMillis();
        Writer writer = new FileWriter("C:\\Users\\Patrick\\Projects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\results\\" + timeStamp + ".json");
        gson.toJson(resultObject, writer);
        writer.flush();
        writer.close();
        System.out.println("[JsonExporter] - Results exported (" + timeStamp + ".json)");
    }

    public void exportInitialLoadToJsonFile(LoadData data) throws IOException {
        Writer writer = new FileWriter("C:\\Users\\Patrick\\Projects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\workload\\initialLoad.json");
        gson.toJson(data, writer);
        writer.flush();
        writer.close();
        System.out.println("[JsonExporter] - Initial load exported: initialLoad.json");
    }
}
