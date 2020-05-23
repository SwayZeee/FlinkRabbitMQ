package com.baqend.core.load.data;

import com.baqend.core.load.data.LoadData;
import com.baqend.core.load.data.SingleDataSet;
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
        ArrayList<SingleDataSet> singleDataSets = new ArrayList<SingleDataSet>();
        for (int i = 1; i <= 10000; i++) {
            SingleDataSet singleDataSet = new SingleDataSet(UUID.randomUUID(), randomDataGenerator.generateRandomDataset(i));
            singleDataSets.add(singleDataSet);
        }
        LoadData loadData = new LoadData();
        loadData.setLoad(singleDataSets);
        return loadData;
    }
}
