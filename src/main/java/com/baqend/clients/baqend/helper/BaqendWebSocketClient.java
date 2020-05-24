package com.baqend.clients.baqend.helper;

import com.baqend.clients.ClientChangeEvent;
import com.google.gson.Gson;
import io.reactivex.rxjava3.subjects.ReplaySubject;

import javax.websocket.*;
import java.io.IOException;
import java.net.URI;

@ClientEndpoint
public class BaqendWebSocketClient {
    private final Gson gson = new Gson();

    private Session userSession = null;
    private final ReplaySubject<ClientChangeEvent> replaySubject;

    public BaqendWebSocketClient(URI endpointURI, ReplaySubject<ClientChangeEvent> replaySubject) {
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        try {
            container.connectToServer(this, endpointURI);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.replaySubject = replaySubject;
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
                ClientChangeEvent clientChangeEvent = new ClientChangeEvent(baqendInitialQueryResult.getId(), baqendInitialQueryResult.getId(), "result");
                replaySubject.onNext(clientChangeEvent);
                return;
            }
            BaqendQueryResult baqendQueryResult = gson.fromJson(message, BaqendQueryResult.class);
            ClientChangeEvent clientChangeEvent = new ClientChangeEvent(baqendQueryResult.getId(), baqendQueryResult.getData().getTransactionID(), baqendQueryResult.getMatchType());
            replaySubject.onNext(clientChangeEvent);
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
