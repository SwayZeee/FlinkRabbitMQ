package com.baqend.core;

import com.baqend.clients.baqend.BaqendClient;
import com.baqend.clients.Client;
import com.baqend.clients.flink.FlinkClient;
import com.baqend.config.Config;
import com.baqend.core.load.LoadGenerator;
import com.baqend.core.measurement.LatencyMeasurement;
import com.baqend.core.subscription.SubscriptionOrchestrator;
import com.baqend.core.subscription.queries.ExampleQuery;
import com.baqend.core.subscription.queries.Query;
import com.google.gson.Gson;

import java.io.FileReader;

/**
 *
 */
public class Benchmark {

    public static void main(String[] args) throws Exception {
        System.out.println();
        System.out.println(" ___  ___  ___  ___       ___  ___       ___                 _                     _   ");
        System.out.println("| . \\|_ _|| . \\| . > ___ / __>| . \\ ___ | . > ___ ._ _  ___ | |_ ._ _ _  ___  _ _ | |__");
        System.out.println("|   / | | | | || . \\|___|\\__ \\|  _/|___|| . \\/ ._>| ' |/ | '| . || ' ' |<_> || '_>| / /");
        System.out.println("|_\\_\\ |_| |___/|___/     <___/|_|       |___/\\___.|_|_|\\_|_.|_|_||_|_|_|<___||_|  |_\\_\\");
        System.out.println();

        Gson gson = new Gson();
        Config config = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\config\\config.json"), Config.class);

        Client client;
        switch (config.clientToTest) {
            case 1:
                client = new BaqendClient();
                break;
            case 2:
                client = new FlinkClient();
                break;
            default:
                throw new Exception("Invalid configuration.");
        }

        LoadGenerator loadGenerator = new LoadGenerator(client, config);
        if (config.isPerformingLoad) {
            loadGenerator.load();
        }
        if (config.isPerformingWarmUp) {
            loadGenerator.warmUp();
        }

        LatencyMeasurement latencyMeasurement = new LatencyMeasurement(config);

        Query query = new ExampleQuery();
        SubscriptionOrchestrator subscriptionOrchestrator = new SubscriptionOrchestrator(client, config);
        subscriptionOrchestrator.doQuerySubscriptions(1, query);

        loadGenerator.start();

        subscriptionOrchestrator.undoQuerySubscriptions();

        if (config.isPerformingCleanUp) {
            loadGenerator.cleanUp();
        }

        latencyMeasurement.doCalculationsAndExport();

        subscriptionOrchestrator.stop();
        loadGenerator.stop();
        latencyMeasurement.stop();
    }
}
