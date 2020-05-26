package com.baqend.workload.generators;

import com.baqend.config.Config;
import com.baqend.core.subscription.query.Query;
import com.baqend.core.subscription.query.QuerySet;
import com.baqend.workload.LoadData;
import com.baqend.workload.SingleDataSet;
import com.baqend.workload.Workload;
import com.baqend.workload.WorkloadEvent;
import com.baqend.workload.WorkloadEventType;
import com.baqend.utils.JsonExporter;
import com.baqend.utils.RandomDataGenerator;
import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.UUID;

public class WorkloadCGenerator {

    private static final Gson gson = new Gson();
    private static final RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
    private static final JsonExporter jsonExporter = new JsonExporter();

    public static void main(String[] args) throws FileNotFoundException {
        Config config = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\config\\config.json"), Config.class);
        String workloadName = "workload_c";
        QuerySet querySet = generateQuerySet();
        Workload workload = generateWorkload(config.duration, config.throughput, config.insertProportion, config.updateProportion);
        jsonExporter.exportQuerySetToJsonFile(querySet, workloadName);
        jsonExporter.exportWorkloadToJsonFile(workload, workloadName, config.throughput);
    }

    public static QuerySet generateQuerySet() {
        QuerySet querySet = new QuerySet();
        for (int i = 1; i <= 100; i++) {
            Query numberQuery = new Query("{\\\"number\\\": " + i + "}", "");
            querySet.addQuery(numberQuery);
        }
        return querySet;
    }

    public static Workload generateWorkload(int duration, int throughput, int insertProportion, int updateProportion) throws FileNotFoundException {
        Workload initialWorkloadData = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\generated\\workloads\\initialLoad.json"), Workload.class);
        LoadData relevantTupels = new LoadData();
        LoadData irrelevantTupels = new LoadData();
        Workload workload = new Workload();

        for (WorkloadEvent workloadEvent : initialWorkloadData.getWorkload()) {
            // TODO: perform check for relevant data tupels in initial load
            if (Integer.parseInt(workloadEvent.getSingleDataSet().getData().get("number")) <= 100) {
                relevantTupels.addSingleDataSet(workloadEvent.getSingleDataSet());
            } else {
                irrelevantTupels.addSingleDataSet(workloadEvent.getSingleDataSet());
            }
        }

        for (int i = 1; i <= (duration * throughput); i++) {
            int randomNumber = randomDataGenerator.generateRandomInteger(1, 100);
            UUID transactionID = UUID.randomUUID();
            // measurement relevant events
            // TODO: create for a lower change event measurement?
            if (i % (throughput / 100) == 0) {
                // updates only, no relevant inserts or deletes
                int randomIndex = randomDataGenerator.generateRandomInteger(0, relevantTupels.getLoad().size() - 1);
                SingleDataSet singleDataSet = relevantTupels.getLoad().get(randomIndex);
                HashMap<String, String> data = randomDataGenerator.generateRandomDataset(Integer.parseInt(singleDataSet.getData().get("number")));
                SingleDataSet newSingleDataSet = new SingleDataSet(singleDataSet.getUuid(), data);

                WorkloadEvent newWorkloadEvent = new WorkloadEvent(transactionID, WorkloadEventType.UPDATE, true, newSingleDataSet);
                workload.addWorkloadEvent(newWorkloadEvent);
            } else {
                if (randomNumber <= insertProportion) {
                    HashMap<String, String> data = randomDataGenerator.generateRandomDataset(initialWorkloadData.getWorkload().size() + 1);
                    SingleDataSet newSingleDataSet = new SingleDataSet(UUID.randomUUID(), data);

                    irrelevantTupels.addSingleDataSet(newSingleDataSet);
                    WorkloadEvent newWorkloadEvent = new WorkloadEvent(transactionID, WorkloadEventType.INSERT, false, newSingleDataSet);
                    initialWorkloadData.addWorkloadEvent(newWorkloadEvent);
                    workload.addWorkloadEvent(newWorkloadEvent);
                } else if (randomNumber <= insertProportion + updateProportion) {
                    int randomIndex = randomDataGenerator.generateRandomInteger(0, irrelevantTupels.getLoad().size() - 1);
                    SingleDataSet singleDataSet = irrelevantTupels.getLoad().get(randomIndex);
                    HashMap<String, String> data = randomDataGenerator.generateRandomDataset(Integer.parseInt(singleDataSet.getData().get("number")));
                    SingleDataSet newSingleDataSet = new SingleDataSet(singleDataSet.getUuid(), data);

                    WorkloadEvent newWorkloadEvent = new WorkloadEvent(transactionID, WorkloadEventType.UPDATE, false, newSingleDataSet);
                    workload.addWorkloadEvent(newWorkloadEvent);
                }
            }
        }
        return workload;
    }
}
