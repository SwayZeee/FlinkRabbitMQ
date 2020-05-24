package com.baqend.clients.baqend.helper;

public class BaqendQueryBuilder {

    public BaqendQueryBuilder() {
    }

    public String composeSubscribeQueryString(String userSession, String query) {
        // TODO: do the query mapping from SQL to baqend query string
//        return "{\n" +
//                "  \"id\": \"" + userSession + "\",\n" +
//                "  \"type\": \"subscribe\",\n" +
//                "  \"token\": null,\n" +
//                "  \"initial\": true,\n" +
//                "  \"bucket\": \"Test\",\n" +
//                "  \"query\": \"{\\\"fieldOne\\\": 1}\",\n" +
//                "  \"operations\": [\n" +
//                "    \"any\"\n" +
//                "  ],\n" +
//                "  \"matchTypes\": [\n" +
//                "    \"all\"\n" +
//                "  ]\n}";

        String queryString = "{\n" +
                "  \"id\": \"" + userSession + "\",\n" +
                "  \"type\": \"subscribe\",\n" +
                "  \"token\": null,\n" +
                "  \"initial\": true,\n";
        queryString = queryString.concat("  \"bucket\": \"Test\",\n");
        queryString = queryString.concat(
                "  \"query\": \"" + query + "\",\n");
        queryString = queryString.concat("  \"operations\": [\n" +
                "    \"any\"\n" +
                "  ],\n" +
                "  \"matchTypes\": [\n" +
                "    \"all\"\n" +
                "  ]\n}");

        return queryString;
    }

    public String composeUnsubscribeQueryString(String userSession) {
        return "{\n" +
                "  \"id\": \"" + userSession + "\",\n" +
                "  \"type\": \"unsubscribe\"\n" +
                "  }";
    }
}

