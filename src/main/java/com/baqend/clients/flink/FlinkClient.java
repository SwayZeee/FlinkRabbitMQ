package com.baqend.clients.flink;

import com.baqend.clients.Client;
import com.baqend.clients.ClientChangeEvent;
import com.baqend.clients.flink.helper.FlinkRMQMessageReceiver;
import com.baqend.clients.flink.helper.FlinkRMQMessageSender;
import com.baqend.core.subscription.query.Query;
import io.reactivex.rxjava3.subjects.ReplaySubject;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class FlinkClient implements Client {

    private final FlinkRMQMessageSender flinkRmqMessageSender = new FlinkRMQMessageSender();
    private FlinkRMQMessageReceiver flinkRMQMessageReceiver;
    private final ReplaySubject<ClientChangeEvent> replaySubject;

    public FlinkClient(ReplaySubject<ClientChangeEvent> replaySubject) throws IOException, TimeoutException {
        this.replaySubject = replaySubject;
    }

    @Override
    public void subscribeQuery(UUID queryID, Query query) {
        try {
            flinkRMQMessageReceiver = new FlinkRMQMessageReceiver(replaySubject);
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
        replaySubject.onComplete();
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
