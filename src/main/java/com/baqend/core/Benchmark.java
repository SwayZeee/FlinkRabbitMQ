package com.baqend.core;

import com.baqend.client.baqend.BaqendClient;
import com.baqend.client.Client;
import com.baqend.client.flink.FlinkClient;
import com.baqend.config.ConfigObject;
import com.baqend.query.ExampleQuery;
import com.baqend.query.Query;
import com.google.gson.Gson;

import java.io.FileReader;

public class Benchmark {

    public static void main(String[] args) throws Exception {
        System.out.println();
        System.out.println(" ___  ___  ___  ___       ___  ___       ___                 _                     _   ");
        System.out.println("| . \\|_ _|| . \\| . > ___ / __>| . \\ ___ | . > ___ ._ _  ___ | |_ ._ _ _  ___  _ _ | |__");
        System.out.println("|   / | | | | || . \\|___|\\__ \\|  _/|___|| . \\/ ._>| ' |/ | '| . || ' ' |<_> || '_>| / /");
        System.out.println("|_\\_\\ |_| |___/|___/     <___/|_|       |___/\\___.|_|_|\\_|_.|_|_||_|_|_|<___||_|  |_\\_\\");
        System.out.println();

        Gson gson = new Gson();
        ConfigObject configObject = gson.fromJson(new FileReader("C:\\Users\\Patrick\\Projects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\config\\config.json"), ConfigObject.class);

        Client client;
        switch (configObject.clientToTest) {
            case 1:
                client = new BaqendClient();
                break;
            case 2:
                client = new FlinkClient();
                break;
            default:
                throw new Exception("Invalid configuration.");
        }

        LoadGenerator loadGenerator = new LoadGenerator(client, configObject);
        if (configObject.isPerformingLoad) {
            loadGenerator.load();
        }
        if (configObject.isPerformingWarmUp) {
            loadGenerator.warmUp();
        }

        LatencyMeasurement latencyMeasurement = new LatencyMeasurement(configObject);

        Query query = new ExampleQuery();
        QueryOrchestrator queryOrchestrator = new QueryOrchestrator(client);
        queryOrchestrator.doQuerySubscriptions(1, query);

        loadGenerator.start();

        queryOrchestrator.undoQuerySubscriptions();

        if (configObject.isPerformingCleanUp) {
            loadGenerator.cleanUp();
        }

        latencyMeasurement.doCalculationsAndExport();

        queryOrchestrator.stop();
        loadGenerator.stop();
        latencyMeasurement.stop();
    }
}
