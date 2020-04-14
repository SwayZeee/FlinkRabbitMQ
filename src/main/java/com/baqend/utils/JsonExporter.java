package com.baqend.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.UUID;

public class JsonExporter {
    private Gson gson;

    public JsonExporter() {
        gson = new GsonBuilder().setPrettyPrinting().create();
    }

    public void exportToJsonFile(Map<UUID, Long> data) throws IOException {
        Writer writer = new FileWriter("C:\\Users\\RüschenbaumPatrickIn\\IdeaProjects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\results\\baqendResult.json");
        gson.toJson(data, writer);
        writer.flush();
        writer.close();
        System.out.println("Results exported");
    }
}
