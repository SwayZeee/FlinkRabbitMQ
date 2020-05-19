package com.baqend.client.baqend;

import com.baqend.messaging.RMQLatencySender;
import com.google.gson.Gson;

import javax.websocket.*;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeoutException;

@ClientEndpoint
public class WebSocketClient {
    public Session userSession = null;
    private final RMQLatencySender rmqLatencySender = new RMQLatencySender();
    private final Gson gson = new Gson();

    public WebSocketClient(URI endpointURI) throws IOException, TimeoutException {
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
        rmqLatencySender.close();
        System.out.println("WS closed: " + reason.toString());
    }

    @OnMessage
    public void onMessage(String message) {
        try {
            if (message.contains("\"type\":\"result\"")) {
                //LatencyMeasurement.getInstance().tock();
                rmqLatencySender.sendMessage("tock" + "," + 1 + "," + " " + "," + System.nanoTime());
                return;
            }
            //System.out.println(message);
            QueryResult queryResult = gson.fromJson(message, QueryResult.class);
            //LatencyMeasurement.getInstance().tock(UUID.fromString(queryResult.getTransactionID()));
            rmqLatencySender.sendMessage("tock" + "," + 0 + "," + queryResult.getTransactionID() + "," + System.nanoTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String message) {
        this.userSession.getAsyncRemote().sendText(message);
    }
}
