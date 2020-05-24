package com.baqend.clients.baqend.helper;

public class BaqendQueryResultData {
    private final String transactionID;

    public BaqendQueryResultData(String transactionID) {
        this.transactionID = transactionID;
    }

    public String getTransactionID() {
        return transactionID;
    }
}
