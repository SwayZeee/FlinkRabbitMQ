package com.baqend.utils;

import com.baqend.workload.LoadData;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.UUID;

public class JsonExporter {
    private final Gson gson;

    public JsonExporter() {
        gson = new GsonBuilder().setPrettyPrinting().create();
    }

    public void exportLatenciesToJsonFile(HashMap<UUID, Long> data) throws IOException {
        long timeStamp = System.currentTimeMillis();
        Writer writer = new FileWriter("src\\main\\java\\com\\baqend\\results\\" + timeStamp + ".json");
        gson.toJson(data, writer);
        writer.flush();
        writer.close();
        System.out.println("Results exported: " + timeStamp + ".json");
    }

    public void exportInitialLoadToJsonFile(LoadData data) throws IOException {
        Writer writer = new FileWriter("src\\main\\java\\com\\baqend\\workload\\initialLoad.json");
        gson.toJson(data, writer);
        writer.flush();
        writer.close();
        System.out.println("Initial load exported: initialLoad.json");
    }
}
