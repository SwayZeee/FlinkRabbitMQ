package com.baqend.clients.baqend.helper;

import com.baqend.core.measurement.LatencyMeasurement;
import com.google.gson.Gson;

import javax.websocket.*;
import java.io.IOException;
import java.net.URI;

@ClientEndpoint
public class BaqendWebSocketClient {
    private final Gson gson = new Gson();

    private Session userSession = null;

    public BaqendWebSocketClient(URI endpointURI) {
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
    }

    @OnClose
    public void onClose(Session userSession, CloseReason reason) {
        this.userSession = null;
    }

    @OnMessage
    public void onMessage(String message) {
        try {
            if (message.contains("\"type\":\"result\"")) {
                BaqendInitialQueryResult baqendInitialQueryResult = gson.fromJson(message, BaqendInitialQueryResult.class);
                LatencyMeasurement.getInstance().tock(baqendInitialQueryResult.getId() + "," + baqendInitialQueryResult.getId(), System.nanoTime());
                return;
            }
            BaqendQueryResult baqendQueryResult = gson.fromJson(message, BaqendQueryResult.class);
            LatencyMeasurement.getInstance().tock(baqendQueryResult.getId() + "," + baqendQueryResult.getData().getTransactionID(), System.nanoTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String message) {
        this.userSession.getAsyncRemote().sendText(message);
    }

    public void close() {
        try {
            this.userSession.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
