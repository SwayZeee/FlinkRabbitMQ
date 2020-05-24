package com.baqend.clients.baqend;

import com.baqend.clients.Client;
import com.baqend.clients.ClientChangeEvent;
import com.baqend.clients.baqend.helper.BaqendQueryBuilder;
import com.baqend.clients.baqend.helper.BaqendRequestBuilder;
import com.baqend.clients.baqend.helper.BaqendWebSocketClient;
import com.baqend.core.subscription.query.Query;
import com.baqend.utils.AHCAsyncHttpClient;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.reactivex.rxjava3.subjects.ReplaySubject;
import org.bson.Document;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.UUID;

public class BaqendClient implements Client {

    private final String BAQEND_WEBSOCKET_URI = "ws://localhost:8080/v1/events";
    private final String BAQEND_HTTP_BASE_URI = "http://localhost:8080/v1";

    private final BaqendQueryBuilder baqendQueryBuilder = new BaqendQueryBuilder();
    private final BaqendRequestBuilder baqendRequestBuilder = new BaqendRequestBuilder();

    private BaqendWebSocketClient baqendWebSocketClient;
    private final ReplaySubject<ClientChangeEvent> replaySubject;

    public BaqendClient(ReplaySubject<ClientChangeEvent> replaySubject) {
        this.replaySubject = replaySubject;
        try {
            baqendWebSocketClient = new BaqendWebSocketClient(new URI(BAQEND_WEBSOCKET_URI), replaySubject);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void subscribeQuery(UUID queryID, Query query) {
        String queryString = baqendQueryBuilder.composeSubscribeQueryString(queryID.toString(), query.getBaqendQuery());
        baqendWebSocketClient.sendMessage(queryString);
    }

    @Override
    public void unsubscribeQuery(UUID queryID) {
        String queryString = baqendQueryBuilder.composeUnsubscribeQueryString(queryID.toString());
        baqendWebSocketClient.sendMessage(queryString);
    }

    @Override
    public void insert(String table, String key, HashMap<String, String> values, UUID transactionID) {
        AHCAsyncHttpClient.getInstance().post(BAQEND_HTTP_BASE_URI + "/db/" + table,
                baqendRequestBuilder.composeRequestString(table, key, values, transactionID)
        );
    }

    @Override
    public void update(String table, String key, HashMap<String, String> values, UUID transactionID) {
        AHCAsyncHttpClient.getInstance().put(BAQEND_HTTP_BASE_URI + "/db/" + table + "/" + key,
                baqendRequestBuilder.composeRequestString(table, key, values, transactionID)
        );
    }

    @Override
    public void delete(String table, String key, UUID transactionID) {
        AHCAsyncHttpClient.getInstance().delete(BAQEND_HTTP_BASE_URI + "/db/" + table + "/" + key);
    }

    @Override
    public void cleanUp(String table) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("local");
        MongoCollection<Document> collection = db.getCollection(table);
        collection.drop();
    }

    @Override
    public void close() {
        replaySubject.onComplete();
        baqendWebSocketClient.close();
        AHCAsyncHttpClient.getInstance().stop();
    }
}
