package com.baqend.client;

import com.baqend.workload.LoadData;

import java.util.HashMap;
import java.util.UUID;

public interface Client {
    void doQuery(String query);

    void setup(String table, LoadData loadData);

    void warmUp();

    void insert(String table, String key, HashMap<String, String> values, UUID transactionID);

    void update(String table, String key, HashMap<String, String> values, UUID transactionID);

    void delete(String table, String key, UUID transactionID);

    void cleanUp(String table);
}
