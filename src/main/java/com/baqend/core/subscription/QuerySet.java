package com.baqend.core.subscription;

import com.baqend.core.subscription.queries.Query;

import java.util.ArrayList;

public class QuerySet {

    private ArrayList<Query> queries = new ArrayList<>();

    public QuerySet() {

    }

    public ArrayList<Query> getQueries() {
        return queries;
    }

    public void addQuery(Query query) {
        queries.add(query);
    }

}
