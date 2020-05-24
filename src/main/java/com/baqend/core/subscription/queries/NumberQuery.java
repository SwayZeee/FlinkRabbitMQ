package com.baqend.core.subscription.queries;

public class NumberQuery implements Query {

    private int number;

    public NumberQuery(int number) {
        this.number = number;
    }

    @Override
    public String getQuery() {
        return "{\\\"number\\\": " + number + "}";
    }
}
