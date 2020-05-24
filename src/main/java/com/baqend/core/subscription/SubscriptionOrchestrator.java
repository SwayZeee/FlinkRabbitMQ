package com.baqend.core.subscription;

import com.baqend.clients.Client;
import com.baqend.config.Config;
import com.baqend.core.measurement.LatencyMeasurement;
import com.baqend.core.subscription.queries.Query;

import java.util.ArrayList;
import java.util.UUID;

public class SubscriptionOrchestrator {

    private ArrayList<UUID> queryIDs = new ArrayList<UUID>();

    private final Client client;
    private final Config config;

    public SubscriptionOrchestrator(Client client, Config config) {
        this.client = client;
        this.config = config;
    }

    public void doQuerySubscriptions(QuerySet querySet) {
        System.out.println("[SubscriptionOrchestrator] - Performing Query Subscription");
        for (Query query: querySet.getQueries()) {
            UUID queryID = UUID.randomUUID();
            subscribeQuery(queryID, query);
        }
        System.out.println("[SubscriptionOrchestrator] - Query Subscription done");
        try {
            Thread.sleep(config.waitingTime);
        } catch (
                Exception e) {
            e.printStackTrace();
        }
    }

    public void undoQuerySubscriptions() {
        System.out.println("[SubscriptionOrchestrator] - Performing Query Unsubscription");
        for (UUID queryID : queryIDs) {
            unsubscribeQuery(queryID);
        }
        queryIDs.clear();
        System.out.println("[SubscriptionOrchestrator] - Query Unsubscription done");
    }

    private void subscribeQuery(UUID queryID, Query query) {
        if (config.isMeasuringInitialResult) {
            LatencyMeasurement.getInstance().tick(queryID.toString(), System.nanoTime());
        }
        client.subscribeQuery(queryID, query.getQuery());
        queryIDs.add(queryID);
    }

    private void unsubscribeQuery(UUID queryID) {
        client.unsubscribeQuery(queryID);
    }
}
