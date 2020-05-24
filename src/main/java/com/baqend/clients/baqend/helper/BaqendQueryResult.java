package com.baqend.clients.baqend.helper;

import java.util.Date;
import java.util.UUID;

public class BaqendQueryResult {
    private final String id;
    private final Date date;
    private final String match;
    private final String matchType;
    private final String operation;
    private final BaqendQueryResultData data;

    public BaqendQueryResult(String id, Date date, String match, String matchType, String operation, BaqendQueryResultData data) {
        this.id = id;
        this.date = date;
        this.match = match;
        this.matchType = matchType;
        this.operation = operation;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public Date getDate() {
        return date;
    }

    public String getMatch() {
        return match;
    }

    public String getMatchType() {
        return matchType;
    }

    public String getOperation() {
        return operation;
    }

    public BaqendQueryResultData getData() {
        return data;
    }
}
