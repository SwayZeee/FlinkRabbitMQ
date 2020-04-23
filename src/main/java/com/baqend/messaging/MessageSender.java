package com.baqend.messaging;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class MessageSender {

    private static MessageSender single_instance = null;
    private Connection connection;
    private Channel channel;
    private String EXCHANGE_NAME = "benchmark";

    private MessageSender() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
    }

    public static MessageSender getInstance() throws IOException, TimeoutException {
        if (single_instance == null) {
            single_instance = new MessageSender();
        }
        return single_instance;
    }

    public void sendMessage(String message) throws IOException {
        channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
        // System.out.println(" [x] Sent '" + message + "'");
    }

    public void closeChannel() throws IOException, TimeoutException {
        channel.close();
    }

    public void closeConnection() throws IOException {
        connection.close();
    }
}
