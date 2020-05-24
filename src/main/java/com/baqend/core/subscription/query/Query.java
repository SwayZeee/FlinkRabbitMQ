package com.baqend.core.subscription.query;

public class Query {

    private final String baqendQuery;
    private final String flinkQuery;

    public Query(String baqendQuery, String flinkQuery) {
        this.baqendQuery = baqendQuery;
        this.flinkQuery = flinkQuery;
    }

    public String getBaqendQuery() {
        return baqendQuery;
    }

    public String getFlinkQuery() {
        return flinkQuery;
    }
}
