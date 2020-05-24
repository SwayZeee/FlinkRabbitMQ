package com.baqend.workload;

import java.util.UUID;

public class WorkloadEvent {
    private UUID transactionID;
    private final WorkloadEventType type;
    private final boolean relevant;
    private SingleDataSet singleDataSet;

    // workload event for inserts and updates
    public WorkloadEvent(UUID transactionID, WorkloadEventType type, boolean relevant, SingleDataSet singleDataSet) {
        this.transactionID = transactionID;
        this.type = type;
        this.relevant = relevant;
        this.singleDataSet = singleDataSet;
    }

    // workload event for deletes
    public WorkloadEvent(UUID transactionID, WorkloadEventType type, boolean relevant) {
        this.transactionID = transactionID;
        this.type = type;
        this.relevant = relevant;
    }

    public UUID getTransactionID() {
        return transactionID;
    }

    public void setTransactionID(UUID transactionID) {
        this.transactionID = transactionID;
    }

    public WorkloadEventType getType() {
        return type;
    }

    public boolean isRelevant() {
        return relevant;
    }

    public SingleDataSet getSingleDataSet() {
        return singleDataSet;
    }

}
