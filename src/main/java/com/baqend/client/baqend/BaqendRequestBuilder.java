package com.baqend.client.baqend;

import java.util.HashMap;
import java.util.UUID;

public class BaqendRequestBuilder {

    public BaqendRequestBuilder() {
    }

    public String composeRequestString(String table, String key, HashMap<String, String> values, UUID transactionID) {
        String request = "{\n ";
        final StringBuilder stringBuilder = new StringBuilder();
        values.forEach((k, v) -> stringBuilder.append("\"").append(k).append("\": \"").append(v).append("\",\n "));
        stringBuilder.append("\"transactionID\": \"").append(transactionID.toString()).append("\",\n ");
        request = request.concat(stringBuilder.toString());
        request = request.concat("\"id\": \"/db/" + table + "/" + key + "\"\n");
        request = request.concat("}");
        return request;
    }
}
