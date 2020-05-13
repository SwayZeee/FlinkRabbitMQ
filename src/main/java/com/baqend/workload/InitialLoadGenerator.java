package com.baqend.workload;

import com.baqend.utils.JsonExporter;
import com.baqend.utils.RandomDataGenerator;

import java.util.ArrayList;
import java.util.UUID;

public class InitialLoadGenerator {

    public static void main(String[] args) {
        JsonExporter jsonExporter = new JsonExporter();
        try {
            jsonExporter.exportInitialLoadToJsonFile(generateInitialLoad());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static LoadData generateInitialLoad() {
        RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
        ArrayList<LoadDataSet> loadDataSets = new ArrayList<LoadDataSet>();
        for (int i = 1; i <= 100000; i++) {
            LoadDataSet loadDataSet = new LoadDataSet(UUID.randomUUID(), randomDataGenerator.generateRandomDataset(i));
            loadDataSets.add(loadDataSet);
        }
        LoadData loadData = new LoadData();
        loadData.setLoad(loadDataSets);
        return loadData;
    }
}
