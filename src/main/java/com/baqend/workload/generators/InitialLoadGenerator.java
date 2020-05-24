package com.baqend.workload.generators;

import com.baqend.workload.SingleDataSet;
import com.baqend.workload.Workload;
import com.baqend.workload.WorkloadEvent;
import com.baqend.workload.WorkloadEventType;
import com.baqend.utils.JsonExporter;
import com.baqend.utils.RandomDataGenerator;

import java.util.UUID;

public class InitialLoadGenerator {

    public static void main(String[] args) {
        JsonExporter jsonExporter = new JsonExporter();
        try {
            jsonExporter.exportInitialLoadToJsonFile(generateInitialLoad(), "initialLoad");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Workload generateInitialLoad() {
        RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
        Workload workload = new Workload();

        for (int i = 1; i <= 100000; i++) {
            UUID key = UUID.randomUUID();
            SingleDataSet singleDataSet = new SingleDataSet(key, randomDataGenerator.generateRandomDataset(i));
            UUID transactionID = UUID.randomUUID();
            WorkloadEvent workloadEvent = new WorkloadEvent(transactionID, WorkloadEventType.INSERT, false, singleDataSet);
            workload.addWorkloadEvent(workloadEvent);
        }
        return workload;
    }
}
