package com.baqend.clients;

public class ClientChangeEvent {
    private String queryID;
    private String transactionID;
    private String type;

    public ClientChangeEvent(String queryID, String transactionID, String type) {
        this.queryID = queryID;
        this.transactionID = transactionID;
        this.type = type;
    }

    public String getQueryID() {
        return queryID;
    }

    public void setQueryID(String queryID) {
        this.queryID = queryID;
    }

    public String getTransactionID() {
        return transactionID;
    }

    public void setTransactionID(String transactionID) {
        this.transactionID = transactionID;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}
