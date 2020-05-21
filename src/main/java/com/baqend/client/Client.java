package com.baqend.client;

import java.util.HashMap;
import java.util.UUID;

public interface Client {
    void subscribeQuery(UUID queryID, String query);

    void unsubscribeQuery(UUID queryID);

    void insert(String table, String key, HashMap<String, String> values, UUID transactionID);

    void update(String table, String key, HashMap<String, String> values, UUID transactionID);

    void delete(String table, String key, UUID transactionID);

    void cleanUp(String table);

    void close();
}
