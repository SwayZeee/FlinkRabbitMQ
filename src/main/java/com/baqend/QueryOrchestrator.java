package com.baqend;

import com.baqend.client.Client;
import com.baqend.query.Query;

import java.util.UUID;

public class QueryOrchestrator {

    private Query query;
    private Client client;

    public QueryOrchestrator(Query query, Client client) {
        this.query = query;
        this.client = client;
    }

    public void registerQuery() {
        UUID uuid = UUID.randomUUID();
        LatencyMeasurement.getInstance().tick(uuid);
        LatencyMeasurement.getInstance().setInitialTick(uuid);
        client.doQuery(query.getQuery());
    }

}
