package com.baqend.workload;

import com.baqend.config.ConfigObject;
import com.baqend.utils.JsonExporter;
import com.baqend.utils.RandomDataGenerator;
import com.google.gson.Gson;
import org.graalvm.compiler.nodes.memory.MemoryCheckpoint;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

public class WorkloadGenerator {

    private static final Gson gson = new Gson();
    private static final RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
    private static final JsonExporter jsonExporter = new JsonExporter();

    public static void main(String[] args) throws FileNotFoundException {
        ConfigObject configObject = gson.fromJson(new FileReader("C:\\Users\\Patrick\\Projects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\config\\config.json"), ConfigObject.class);

        generateWorkload(configObject.duration, configObject.throughput, configObject.insertProportion, configObject.updateProportion);
    }

    public static void generateWorkload(int duration, int throughput, int insertProportion, int updateProportion) throws FileNotFoundException {
        LoadData loadData = gson.fromJson(new FileReader("C:\\Users\\Patrick\\Projects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\workload\\initialLoad.json"), LoadData.class);
        LoadData relevantTupels = new LoadData();
        LoadData irrelevantTupels = new LoadData();
        Workload workload = new Workload();

        for (SingleDataSet singleDataSet: loadData.getLoad()) {
            // TODO: perform check for relevant data tupels in initial load
            if (singleDataSet.getData().get("fieldOne").equals("1")) {
                relevantTupels.addSingleDataSet(singleDataSet);
            } else {
                irrelevantTupels.addSingleDataSet(singleDataSet);
            }
        }

        System.out.println("Number of relevant tupels: " + relevantTupels.getLoad().size());
        System.out.println("Number of irrelevant tupels: "+ irrelevantTupels.getLoad().size());

        for (int i = 1; i <= (duration * throughput); i++) {
            int randomNumber = randomDataGenerator.generateRandomInteger(1, 100);
            UUID transactionID = UUID.randomUUID();
            // measurement relevant events
            if (i % (throughput / 100) == 0) {
                // checked
                if (randomNumber <= insertProportion) {
                    HashMap<String, String> data = new HashMap<String, String>();
                    data.put("fieldOne", Integer.toString(randomDataGenerator.generateRandomInteger(1, 1)));
                    data.put("fieldTwo", Double.toString(randomDataGenerator.generateRandomDouble(1, 1000)));
                    data.put("fieldThree", randomDataGenerator.generateRandomString(6, true, false));
                    data.put("fieldFour", Integer.toString(randomDataGenerator.generateRandomInteger(1001, 10000)));
                    data.put("fieldFive", Double.toString(randomDataGenerator.generateRandomDouble(1001, 10000)));
                    data.put("fieldSix", randomDataGenerator.generateRandomString(12, true, false));
                    data.put("fieldSeven", Integer.toString(randomDataGenerator.generateRandomInteger(10001, 100000)));
                    data.put("fieldEight", Double.toString(randomDataGenerator.generateRandomDouble(10001, 100000)));
                    data.put("fieldNine", randomDataGenerator.generateRandomString(18, true, false));
                    data.put("number", Integer.toString(loadData.getLoad().size() + 1));
                    SingleDataSet newSingleDataSet = new SingleDataSet(UUID.randomUUID(), data);

                    loadData.addSingleDataSet(newSingleDataSet);
                    relevantTupels.addSingleDataSet(newSingleDataSet);
                    WorkloadEvent workloadEvent = new WorkloadEvent(transactionID, WorkloadEventType.INSERT, true, newSingleDataSet);
                    workload.addWorklodEvent(workloadEvent);
                // error!
                } else if (randomNumber <= insertProportion + updateProportion) {
                    int randomIndex = randomDataGenerator.generateRandomInteger(0, loadData.getLoad().size() - 1);
                    SingleDataSet singleDataSet = loadData.getLoad().get(randomIndex);
                    // working?
                    boolean contains = false;
                    for (SingleDataSet temp: relevantTupels.getLoad()) {
                        if (temp.getUuid().toString().equals(singleDataSet.getUuid().toString())) {
                            contains = true;
                        }
                    }
                    if (contains) {
                        // remove notification
                        HashMap<String, String> data = new HashMap<String, String>();
                        data.put("fieldOne", Integer.toString(randomDataGenerator.generateRandomInteger(2, 1000)));
                        data.put("fieldTwo", Double.toString(randomDataGenerator.generateRandomDouble(1, 1000)));
                        data.put("fieldThree", randomDataGenerator.generateRandomString(6, true, false));
                        data.put("fieldFour", Integer.toString(randomDataGenerator.generateRandomInteger(1001, 10000)));
                        data.put("fieldFive", Double.toString(randomDataGenerator.generateRandomDouble(1001, 10000)));
                        data.put("fieldSix", randomDataGenerator.generateRandomString(12, true, false));
                        data.put("fieldSeven", Integer.toString(randomDataGenerator.generateRandomInteger(10001, 100000)));
                        data.put("fieldEight", Double.toString(randomDataGenerator.generateRandomDouble(10001, 100000)));
                        data.put("fieldNine", randomDataGenerator.generateRandomString(18, true, false));
                        data.put("number", singleDataSet.getData().get("number"));
                        SingleDataSet newSingleDataSet = new SingleDataSet(singleDataSet.getUuid(), data);

                        // problem removing from relevantTupels?

                        WorkloadEvent workloadEvent = new WorkloadEvent(transactionID, WorkloadEventType.UPDATE, true, newSingleDataSet);
                        workload.addWorklodEvent(workloadEvent);
                    } else {
                        // update notification
                        HashMap<String, String> data = new HashMap<String, String>();
                        data.put("fieldOne", Integer.toString(randomDataGenerator.generateRandomInteger(1, 1)));
                        data.put("fieldTwo", Double.toString(randomDataGenerator.generateRandomDouble(1, 1000)));
                        data.put("fieldThree", randomDataGenerator.generateRandomString(6, true, false));
                        data.put("fieldFour", Integer.toString(randomDataGenerator.generateRandomInteger(1001, 10000)));
                        data.put("fieldFive", Double.toString(randomDataGenerator.generateRandomDouble(1001, 10000)));
                        data.put("fieldSix", randomDataGenerator.generateRandomString(12, true, false));
                        data.put("fieldSeven", Integer.toString(randomDataGenerator.generateRandomInteger(10001, 100000)));
                        data.put("fieldEight", Double.toString(randomDataGenerator.generateRandomDouble(10001, 100000)));
                        data.put("fieldNine", randomDataGenerator.generateRandomString(18, true, false));
                        data.put("number", singleDataSet.getData().get("number"));
                        SingleDataSet newSingleDataSet = new SingleDataSet(singleDataSet.getUuid(), data);

                        relevantTupels.addSingleDataSet(newSingleDataSet);
                        WorkloadEvent workloadEvent = new WorkloadEvent(transactionID, WorkloadEventType.UPDATE, true, newSingleDataSet);
                        workload.addWorklodEvent(workloadEvent);
                    }
                } else {
                    // WorkloadEventType.DELETE not implemented
                }
            } else {
                HashMap<String, String> data = new HashMap<String, String>();
                data.put("fieldOne", Integer.toString(randomDataGenerator.generateRandomInteger(2, 1000)));
                data.put("fieldTwo", Double.toString(randomDataGenerator.generateRandomDouble(1, 1000)));
                data.put("fieldThree", randomDataGenerator.generateRandomString(6, true, false));
                data.put("fieldFour", Integer.toString(randomDataGenerator.generateRandomInteger(1001, 10000)));
                data.put("fieldFive", Double.toString(randomDataGenerator.generateRandomDouble(1001, 10000)));
                data.put("fieldSix", randomDataGenerator.generateRandomString(12, true, false));
                data.put("fieldSeven", Integer.toString(randomDataGenerator.generateRandomInteger(10001, 100000)));
                data.put("fieldEight", Double.toString(randomDataGenerator.generateRandomDouble(10001, 100000)));
                data.put("fieldNine", randomDataGenerator.generateRandomString(18, true, false));

                if (randomNumber <= insertProportion) {
                    data.put("number", Integer.toString(loadData.getLoad().size() + 1));
                    SingleDataSet newSingleDataSet = new SingleDataSet(UUID.randomUUID(), data);

                    loadData.addSingleDataSet(newSingleDataSet);
                    irrelevantTupels.addSingleDataSet(newSingleDataSet);
                    WorkloadEvent workloadEvent = new WorkloadEvent(transactionID, WorkloadEventType.INSERT, false, newSingleDataSet);
                    workload.addWorklodEvent(workloadEvent);
                } else if (randomNumber <= insertProportion + updateProportion) {
                    int randomIndex = randomDataGenerator.generateRandomInteger(0, irrelevantTupels.getLoad().size() - 1);
                    SingleDataSet singleDataSet = irrelevantTupels.getLoad().get(randomIndex);
                    data.put("number", singleDataSet.getData().get("number"));
                    SingleDataSet newSingleDataSet = new SingleDataSet(singleDataSet.getUuid(), data);

                    WorkloadEvent workloadEvent = new WorkloadEvent(transactionID, WorkloadEventType.UPDATE, false, newSingleDataSet);
                    workload.addWorklodEvent(workloadEvent);
                } else {
                    // WorkloadEventType.DELETE not implemented
                }
            }
        }
        System.out.println("Number of relevant tupels: " + relevantTupels.getLoad().size());
        System.out.println("Number of irrelevant tupels: "+ irrelevantTupels.getLoad().size());
        //TODO: compose workload name of parameters for automatic loading of LoadGenerator?
        jsonExporter.exportWorkloadToJsonFile(workload);
    }
}
