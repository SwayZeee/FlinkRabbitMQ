package com.baqend.core.subscription;

import com.baqend.clients.Client;
import com.baqend.config.Config;
import com.baqend.core.measurement.LatencyMeasurement;
import com.baqend.core.subscription.query.Query;
import com.baqend.core.subscription.query.QuerySet;
import com.google.common.util.concurrent.RateLimiter;

import java.util.ArrayList;
import java.util.UUID;

public class SubscriptionOrchestrator {

    private final ArrayList<UUID> queryIDs = new ArrayList<UUID>();

    private final Client client;
    private final Config config;
    private final LatencyMeasurement latencyMeasurement;

    public SubscriptionOrchestrator(Client client, Config config, LatencyMeasurement latencyMeasurement) {
        this.client = client;
        this.config = config;
        this.latencyMeasurement = latencyMeasurement;
    }

    public void doQuerySubscriptions(QuerySet querySet) {
        System.out.println("[SubscriptionOrchestrator] - Performing Query Subscription (queries: " + querySet.getQueries().size() + " @ 1 ops/s)");
        double queryCounter = 1;
        RateLimiter rateLimiter = RateLimiter.create(1);
        for (Query query : querySet.getQueries()) {
            System.out.print("\r[SubscriptionOrchestrator] - Query Subscription in progess " + (int) (queryCounter / querySet.getQueries().size() * 100) + " %");
            rateLimiter.acquire();
            UUID queryID = UUID.randomUUID();
            subscribeQuery(queryID, query);
            queryCounter++;
        }
        System.out.println("\r[SubscriptionOrchestrator] - Query Subscription done");
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
            latencyMeasurement.tick(queryID.toString());
        }
        client.subscribeQuery(queryID, query);
        queryIDs.add(queryID);
    }

    private void unsubscribeQuery(UUID queryID) {
        client.unsubscribeQuery(queryID);
    }
}
