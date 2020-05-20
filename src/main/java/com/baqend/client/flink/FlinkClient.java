package com.baqend.client.flink;

import com.baqend.config.ConfigObject;
import com.baqend.client.Client;
import com.baqend.messaging.RMQMessageSender;
import com.baqend.workload.LoadData;
import com.baqend.workload.SingleDataSet;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class FlinkClient implements Client {

    private final RMQMessageSender rmqMessageSender = new RMQMessageSender();

    public FlinkClient(ConfigObject configObject) throws IOException, TimeoutException {
    }

    public void subscribeQuery(String query) {
//        FlinkThread flinkThread = new FlinkThread(query);
//        flinkThread.start();
//        // time delay for starting flink
//        try {
//            Thread.sleep(15000);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public void unsubscribeQuery() {
    }

    public void setup(String table, LoadData loadData) {
        for (SingleDataSet singleDataSet : loadData.getLoad()) {
            try {
                rmqMessageSender.sendMessage(singleDataSet.getUuid() + "," + singleDataSet.getData().get("number"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void warmUp() {

    }

    @Override
    public void insert(String table, String key, HashMap<String, String> values, UUID transactionID) {
        try {
            rmqMessageSender.sendMessage(
                    transactionID + "," +
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void update(String table, String key, HashMap<String, String> values, UUID transactionID) {
        try {
            rmqMessageSender.sendMessage(
                    transactionID + "," +
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(String table, String key, UUID transactionID) {
        // not supported
    }

    @Override
    public void cleanUp(String table) {
        rmqMessageSender.close();
    }
}
