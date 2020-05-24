package com.baqend.clients.flink;

import com.baqend.clients.Client;
import com.baqend.clients.flink.helper.FlinkRMQMessageReceiver;
import com.baqend.clients.flink.helper.FlinkRMQMessageSender;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class FlinkClient implements Client {

    private final FlinkRMQMessageSender flinkRmqMessageSender = new FlinkRMQMessageSender();
    private FlinkRMQMessageReceiver flinkRMQMessageReceiver;

    public FlinkClient() throws IOException, TimeoutException {
    }

    @Override
    public void subscribeQuery(UUID queryID, String query) {
        try {
            flinkRMQMessageReceiver = new FlinkRMQMessageReceiver();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void unsubscribeQuery(UUID queryID) {
        flinkRMQMessageReceiver.close();
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
        flinkRmqMessageSender.close();
    }

    private void upsert(String table, String key, HashMap<String, String> values, UUID transactionID) {
        flinkRmqMessageSender.sendMessage(
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
