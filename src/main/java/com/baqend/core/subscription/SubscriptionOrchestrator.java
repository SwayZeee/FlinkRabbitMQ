package com.baqend.core.subscription;

import com.baqend.clients.Client;
import com.baqend.core.subscription.queries.Query;
import com.baqend.messaging.RMQLatencySender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class SubscriptionOrchestrator {

    private final RMQLatencySender rmqLatencySender = new RMQLatencySender();

    private ArrayList<UUID> queryIDs = new ArrayList<UUID>();

    private final Client client;

    public SubscriptionOrchestrator(Client client) throws IOException, TimeoutException {
        this.client = client;
    }

    public void doQuerySubscriptions(int amount, Query query) {
        System.out.println("[SubscriptionOrchestrator] - Performing Query Subscription");
        for (int i = 0; i < amount; i++) {
            UUID queryID = UUID.randomUUID();
            subscribeQuery(queryID, query);
        }
        System.out.println("[SubscriptionOrchestrator] - Query Subscription done");
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
        rmqLatencySender.sendMessage("tick" + "," + 1 + "," + queryID.toString() + "," + System.nanoTime());
        client.subscribeQuery(queryID, query.getQuery());
        queryIDs.add(queryID);
    }

    private void unsubscribeQuery(UUID queryID) {
        client.unsubscribeQuery(queryID);
    }

    public void stop() {
        rmqLatencySender.close();
        System.out.println("[SubscriptionOrchestrator] - Stopped");
    }
}
