package com.baqend.core;

import com.baqend.clients.baqend.BaqendClient;
import com.baqend.clients.Client;
import com.baqend.clients.flink.FlinkClient;
import com.baqend.config.Config;
import com.baqend.core.load.LoadGenerator;
import com.baqend.core.measurement.LatencyMeasurement;
import com.baqend.core.subscription.SubscriptionOrchestrator;
import com.baqend.core.subscription.queries.FieldOneQuery;
import com.baqend.core.subscription.queries.NumberQuery;
import com.baqend.core.subscription.queries.Query;
import com.baqend.core.subscription.QuerySet;
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

        // Generating queries for subscription
        QuerySet querySet = new QuerySet();
        for (int i = 1; i <= 100; i++) {
            Query query = new NumberQuery(i);
            querySet.addQuery(query);
        }

        SubscriptionOrchestrator subscriptionOrchestrator = new SubscriptionOrchestrator(client, config);
        subscriptionOrchestrator.doQuerySubscriptions(querySet);

        loadGenerator.start();

        subscriptionOrchestrator.undoQuerySubscriptions();

        if (config.isPerformingCleanUp) {
            loadGenerator.cleanUp();
        }

        LatencyMeasurement.getInstance().doCalculationsAndExport();

        loadGenerator.stop();
    }
}
