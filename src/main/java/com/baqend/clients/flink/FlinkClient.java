package com.baqend.clients.flink;

import com.baqend.clients.Client;
import com.baqend.messaging.RMQMessageSender;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class FlinkClient implements Client {

    private final RMQMessageSender rmqMessageSender = new RMQMessageSender();

    public FlinkClient() throws IOException, TimeoutException {
    }

    @Override
    public void subscribeQuery(UUID queryID, String query) {
    }

    @Override
    public void unsubscribeQuery(UUID queryID) {
    }

    @Override
    public void insert(String table, String key, HashMap<String, String> values, UUID transactionID) {
        upsert(table, key, values, transactionID);
    }

    @Override
    public void update(String table, String key, HashMap<String, String> values, UUID transactionID) {
        upsert(table, key, values, transactionID);
    }

    @Override
    public void delete(String table, String key, UUID transactionID) {
    }

    @Override
    public void cleanUp(String table) {
    }

    @Override
    public void close() {
        rmqMessageSender.close();
    }

    private void upsert(String table, String key, HashMap<String, String> values, UUID transactionID) {
        rmqMessageSender.sendMessage(
                transactionID.toString() + "," +
                        key + "," +
                        values.get("fieldOne") + "," +
                        values.get("fieldTwo") + "," +
                        values.get("fieldThree") + "," +
                        values.get("fieldFour") + "," +
                        values.get("fieldFive") + "," +
                        values.get("fieldSix") + "," +
                        values.get("fieldSeven") + "," +
                        values.get("fieldEight") + "," +
                        values.get("fieldNine") + "," +
                        values.get("number")
        );
    }
}
