package com.baqend.client.baqend.helper;

import java.util.Date;
import java.util.UUID;

public class BaqendQueryResult {
    private UUID id;
    private Date date;
    private String match;
    private String matchType;
    private String operation;
    private BaqendQueryResultData data;

    public BaqendQueryResult(UUID id, Date date, String match, String matchType, String operation, BaqendQueryResultData data) {
        this.id = id;
        this.date = date;
        this.match = match;
        this.matchType = matchType;
        this.operation = operation;
        this.data = data;
    }

    public UUID getId() {
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
