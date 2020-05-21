package com.baqend.core;

import com.baqend.client.Client;
import com.baqend.messaging.RMQLatencySender;
import com.baqend.query.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class QueryOrchestrator {

    private final RMQLatencySender rmqLatencySender = new RMQLatencySender();

    private ArrayList<UUID> queryIDs = new ArrayList<UUID>();

    private final Client client;

    public QueryOrchestrator(Client client) throws IOException, TimeoutException {
        this.client = client;
    }

    public void doQuerySubscriptions(int amount, Query query) {
        System.out.println("[QueryOrchestrator] - Performing Query Subscription");
        for (int i = 0; i < amount; i++) {
            UUID queryID = UUID.randomUUID();
            subscribeQuery(queryID, query);
        }
        System.out.println("[QueryOrchestrator] - Query Subscription done");
    }

    public void undoQuerySubscriptions() {
        System.out.println("[QueryOrchestrator] - Performing Query Unsubscription");
        for (UUID queryID : queryIDs) {
            unsubscribeQuery(queryID);
        }
        queryIDs.clear();
        System.out.println("[QueryOrchestrator] - Query Unsubscription done");
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
        System.out.println("[QueryOrchestrator] - Stopped");
    }
}
