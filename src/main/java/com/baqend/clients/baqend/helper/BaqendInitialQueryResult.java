package com.baqend.clients.baqend.helper;

import java.util.Date;

public class BaqendInitialQueryResult {
    private String id;
    private Date date;
    private String type;

    public BaqendInitialQueryResult(String id, Date date, String type) {
        this.id = id;
        this.date = date;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public Date getDate() {
        return date;
    }

    public String getType() {
        return type;
    }
}
