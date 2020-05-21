package com.baqend.core;

import com.baqend.client.Client;
import com.baqend.messaging.RMQLatencySender;
import com.baqend.query.Query;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class QueryOrchestrator {

    private final RMQLatencySender rmqLatencySender = new RMQLatencySender();
    private final Query query;
    private final Client client;

    public QueryOrchestrator(Query query, Client client) throws IOException, TimeoutException {
        this.query = query;
        this.client = client;
    }

    public void subscribeQuery() {
        UUID uuid = UUID.randomUUID();
        try {
            rmqLatencySender.sendMessage("tick" + "," + 1 + "," + uuid.toString() + "," + System.nanoTime());
        } catch (IOException e) {
            e.printStackTrace();
        }
        client.subscribeQuery(query.getQuery());
    }

    public void unsubscribeQuery() {
        rmqLatencySender.close();
        client.unsubscribeQuery();
    }
}
