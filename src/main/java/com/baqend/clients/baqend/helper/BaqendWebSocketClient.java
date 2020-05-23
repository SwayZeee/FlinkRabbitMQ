package com.baqend.clients.baqend.helper;

import com.baqend.messaging.RMQLatencySender;
import com.google.gson.Gson;

import javax.websocket.*;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeoutException;

@ClientEndpoint
public class BaqendWebSocketClient {
    private final RMQLatencySender rmqLatencySender = new RMQLatencySender();
    private final Gson gson = new Gson();

    private Session userSession = null;

    public BaqendWebSocketClient(URI endpointURI) throws IOException, TimeoutException {
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        try {
            container.connectToServer(this, endpointURI);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @OnOpen
    public void onOpen(Session userSession) {
        this.userSession = userSession;
        // System.out.println("WS opened: " + userSession.toString());
    }

    @OnClose
    public void onClose(Session userSession, CloseReason reason) {
        this.userSession = null;
        // System.out.println("WS closed: " + reason.toString());
    }

    @OnMessage
    public void onMessage(String message) {
        try {
            if (message.contains("\"type\":\"result\"")) {
                rmqLatencySender.sendMessage("tock" + "," + 1 + "," + " " + "," + System.nanoTime());
                return;
            }
            BaqendQueryResult baqendQueryResult = gson.fromJson(message, BaqendQueryResult.class);
            rmqLatencySender.sendMessage("tock" + "," + 0 + "," + baqendQueryResult.getData().getTransactionID() + "," + System.nanoTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String message) {
        this.userSession.getAsyncRemote().sendText(message);
    }

    public void close() {
        try {
            rmqLatencySender.close();
            this.userSession.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
