package com.baqend.client.baqend;

import com.baqend.core.LatencyMeasurement;
import com.google.gson.Gson;

import javax.websocket.*;
import java.net.URI;
import java.util.UUID;

@ClientEndpoint
public class WebSocketClient {
    Session userSession = null;

    public WebSocketClient(URI endpointURI) {
        try {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            container.connectToServer(this, endpointURI);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
            LatencyMeasurement.getInstance().tock();
            return;
        }
        //System.out.println(message);
        try {
            Gson gson = new Gson();
            QueryResult queryResult = gson.fromJson(message, QueryResult.class);
            LatencyMeasurement.getInstance().tock(UUID.fromString(queryResult.getTransactionID()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String message) {
        this.userSession.getAsyncRemote().sendText(message);
    }
}
