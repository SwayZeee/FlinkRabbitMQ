package com.baqend.core;

import com.baqend.clients.ClientChangeEvent;
import com.baqend.clients.baqend.BaqendClient;
import com.baqend.clients.Client;
import com.baqend.clients.flink.FlinkClient;
import com.baqend.config.Config;
import com.baqend.core.load.LoadGenerator;
import com.baqend.core.measurement.LatencyMeasurement;
import com.baqend.core.subscription.SubscriptionOrchestrator;
import com.baqend.core.subscription.query.QuerySet;
import com.google.gson.Gson;
import io.reactivex.rxjava3.subjects.ReplaySubject;

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

        ReplaySubject<ClientChangeEvent> replaySubject = ReplaySubject.create();

        Client client;
        switch (config.clientToTest) {
            case 1:
                client = new BaqendClient(replaySubject);
                break;
            case 2:
                client = new FlinkClient(replaySubject);
                break;
            default:
                throw new Exception("Invalid configuration.");
        }

        LatencyMeasurement latencyMeasurement = new LatencyMeasurement(config);

        LoadGenerator loadGenerator = new LoadGenerator(client, config, latencyMeasurement);

        if (config.isPerformingLoad) {
            loadGenerator.load();
        }
        if (config.isPerformingWarmUp) {
            loadGenerator.warmUp();
        }

        replaySubject.subscribe(latencyMeasurement);

        QuerySet querySet = gson.fromJson(new FileReader("src\\main\\java\\com\\baqend\\generated\\querysets\\" + config.workload + ".json"), QuerySet.class);
        SubscriptionOrchestrator subscriptionOrchestrator = new SubscriptionOrchestrator(client, config, latencyMeasurement);
        subscriptionOrchestrator.doQuerySubscriptions(querySet);

        loadGenerator.start();

        subscriptionOrchestrator.undoQuerySubscriptions();

        if (config.isPerformingCleanUp) {
            loadGenerator.cleanUp();
        }

        loadGenerator.stop();
    }
}
