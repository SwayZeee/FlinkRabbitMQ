package com.baqend.client.flink;

import com.baqend.config.ConfigObject;
import com.baqend.client.Client;
import com.baqend.messaging.RMQMessageSender;
import com.baqend.utils.HttpClient;
import com.baqend.workload.LoadData;
import com.baqend.workload.LoadDataSet;

import java.util.HashMap;
import java.util.UUID;

public class FlinkClient implements Client {

    public FlinkClient(ConfigObject configObject) {
    }

    public void doQuery(String query) {
        FlinkThread flinkThread = new FlinkThread(query);
        flinkThread.start();
        // time delay for starting flink
        try {
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setup(String table, LoadData loadData) {
        for (LoadDataSet loadDataSet : loadData.getLoad()) {
            try {
                RMQMessageSender.getInstance().sendMessage(loadDataSet.getUuid() + "," + loadDataSet.getData().get("number"));
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
            RMQMessageSender.getInstance().sendMessage(transactionID.toString() + "," + "Patrick");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void update(String table, String key, HashMap<String, String> values, UUID transactionID) {
        try {
            RMQMessageSender.getInstance().sendMessage(transactionID.toString() + "," + "Patrick");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(String table, String key, UUID transactionID) {

    }

    @Override
    public void cleanUp(String table) {

    }
}
