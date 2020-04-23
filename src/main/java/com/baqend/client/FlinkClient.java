package com.baqend.client;

import akka.io.Tcp;
import com.baqend.ConfigObject;
import com.baqend.messaging.MessageReceiver;
import com.baqend.messaging.MessageSender;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class FlinkClient implements Client {

    private ConfigObject configObject;
    private FlinkThread flinkThread;

    public FlinkClient(ConfigObject configObject) throws IOException, TimeoutException {
        this.configObject = configObject;
    }

    public void doQuery(String query) {
        flinkThread = new FlinkThread(query);
        flinkThread.start();
        // time delay for starting flink
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
        }
    }

    public void setup() throws Exception {
//        for (int i = 0; i < 100; i++) {
//            MessageSender.getInstance().sendMessage("Hello World!");
//        }
    }

    public void warmUp() {

    }

    public void updateData(UUID uuid) throws IOException, TimeoutException {
        MessageSender.getInstance().sendMessage("Hello World! " + uuid.toString());
    }

    public void deleteData() {
    }
}
