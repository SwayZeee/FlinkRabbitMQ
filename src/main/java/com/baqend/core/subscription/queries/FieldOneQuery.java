package com.baqend.core.subscription.queries;

public class FieldOneQuery implements Query {
    @Override
    public String getQuery() {
        return "{\\\"fieldOne\\\": 1}";
    }
}
