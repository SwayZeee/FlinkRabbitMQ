package com.baqend.workload;

import java.util.HashMap;
import java.util.UUID;

public class LoadDataSet {
    private UUID uuid;
    private HashMap<String, String> data;

    public LoadDataSet(UUID uuid, HashMap<String, String> data) {
        this.uuid = uuid;
        this.data = data;
    }

    public UUID getUuid() {
        return uuid;
    }

    public HashMap<String, String> getData() {
        return data;
    }
}
