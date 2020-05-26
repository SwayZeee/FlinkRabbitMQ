package com.baqend.clients.flink.helper;

import com.baqend.clients.ClientChangeEvent;
import com.rabbitmq.client.*;
import io.reactivex.rxjava3.subjects.ReplaySubject;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class FlinkRMQMessageReceiver {

    private static final String EXCHANGE_NAME = "flinkChangeEvents";
    private static Connection connection;
    private static Channel channel;

    private final ReplaySubject<ClientChangeEvent> replaySubject;

    public FlinkRMQMessageReceiver(ReplaySubject<ClientChangeEvent> replaySubject) throws IOException, TimeoutException {

        this.replaySubject = replaySubject;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println("Exchange: " + EXCHANGE_NAME + " Queue: " + queueName);

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
                String[] tokens = message.split(",");
                if (Boolean.parseBoolean(tokens[0])) {
                    ClientChangeEvent clientChangeEvent = new ClientChangeEvent(tokens[1], tokens[2], "type");
                    replaySubject.onNext(clientChangeEvent);
                }
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }

    public boolean isChannelOpen() {
        return channel.isOpen();
    }

    public boolean isConnectionOpen() {
        return connection.isOpen();
    }

    public void closeChannel() {
        try {
            channel.close();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public void closeConnection() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        closeChannel();
        closeConnection();
    }

}
