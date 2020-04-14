package com.baqend.client;

import com.baqend.ConfigObject;
import com.baqend.messaging.MessageSender;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class FlinkClient implements Client {

    private ConfigObject configObject;

    public FlinkClient(ConfigObject configObject) {
        this.configObject = configObject;
    }

    public void doQuery(String query) {
        FlinkThread flinkThread = new FlinkThread(query);
        flinkThread.start();
    }

    public void setup() throws Exception {
//        for (int i = 0; i < 100; i++) {
//            MessageSender.getInstance().sendMessage("Hello World!");
//        }
//        MessageSender.getInstance().closeChannel();
//        MessageSender.getInstance().closeConnection();
    }

    public void warmUp() {

    }

    public void updateData(UUID uuid) throws IOException, TimeoutException {
        MessageSender.getInstance().sendMessage("Hello World! " + uuid.toString());
        //MessageSender.getInstance().closeChannel();
        //MessageSender.getInstance().closeConnection();
    }

    public void deleteData() {

    }
}
