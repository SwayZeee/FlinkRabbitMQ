package com.baqend.messaging;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RMQMessageReceiver {

    private Connection connection;
    private Channel channel;
    private static final String EXCHANGE_NAME = "benchmark";

    public RMQMessageReceiver() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" Exchange: " + EXCHANGE_NAME + " Queue: " + queueName);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new Consumer() {
            @Override
            public void handleConsumeOk(String s) {

            }

            @Override
            public void handleCancelOk(String s) {

            }

            @Override
            public void handleCancel(String s) {

            }

            @Override
            public void handleShutdownSignal(String s, ShutdownSignalException e) {

            }

            @Override
            public void handleRecoverOk(String s) {

            }

            @Override
            public void handleDelivery(String s, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] bytes) {
                String message = new String(bytes);
                System.out.println(message);
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }
}
