package com.baqend.core.subscription.query;

import java.util.ArrayList;

public class QuerySet {

    private final ArrayList<Query> queries = new ArrayList<>();

    public QuerySet() {

    }

    public ArrayList<Query> getQueries() {
        return queries;
    }

    public void addQuery(Query query) {
        queries.add(query);
    }

}
