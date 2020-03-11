package com.baqend.utils;

import com.baqend.LatencyMeasurement;
import com.baqend.QueryResult;
import com.google.gson.Gson;

import javax.websocket.*;
import java.net.URI;
import java.util.UUID;

@ClientEndpoint
public class WebSocketClient {
    Session userSession = null;
    LatencyMeasurement latencyMeasurement;

    public WebSocketClient(URI endpointURI) {
        try {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            container.connectToServer(this, endpointURI);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        latencyMeasurement = LatencyMeasurement.getInstance();
    }

    @OnOpen
    public void onOpen(Session userSession) {
        this.userSession = userSession;
        System.out.println("WS opened: " + userSession.toString());
    }

    @OnClose
    public void onClose(Session userSession, CloseReason reason) {
        this.userSession = null;
        System.out.println("WS closed: " + reason.toString());
    }

    @OnMessage
    public void onMessage(String message) {
        if (message.contains("\"type\":\"result\"")) {
            this.latencyMeasurement.tock();
            return;
        }
        Gson gson = new Gson();
        QueryResult queryResult = gson.fromJson(message, QueryResult.class);
        this.latencyMeasurement.tock(UUID.fromString(queryResult.getTestName()));
    }

    public void sendMessage(String message) {
        this.userSession.getAsyncRemote().sendText(message);
    }

    public Session getUserSession() {
        return userSession;
    }
}
