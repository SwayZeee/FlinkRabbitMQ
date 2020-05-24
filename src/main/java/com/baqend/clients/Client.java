package com.baqend.clients;

import com.baqend.core.subscription.query.Query;

import java.util.HashMap;
import java.util.UUID;

public interface Client {

    /**
     *
     * @param queryID
     * @param query
     */
    void subscribeQuery(UUID queryID, Query query);

    /**
     *
     * @param queryID
     */
    void unsubscribeQuery(UUID queryID);

    /**
     *
     * @param table
     * @param key
     * @param values
     * @param transactionID
     */
    void insert(String table, String key, HashMap<String, String> values, UUID transactionID);

    /**
     *
     * @param table
     * @param key
     * @param values
     * @param transactionID
     */
    void update(String table, String key, HashMap<String, String> values, UUID transactionID);

    /**
     *
     * @param table
     * @param key
     * @param transactionID
     */
    void delete(String table, String key, UUID transactionID);

    /**
     *
     * @param table
     */
    void cleanUp(String table);

    /**
     *
     */
    void close();
}
