package com.baqend.core.load.data;

import java.util.HashMap;
import java.util.UUID;

public class SingleDataSet {
    private UUID uuid;
    private HashMap<String, String> data;

    public SingleDataSet(UUID uuid, HashMap<String, String> data) {
        this.uuid = uuid;
        this.data = data;
    }

    public UUID getUuid() {
        return uuid;
    }

    public HashMap<String, String> getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SingleDataSet singleDataSet = (SingleDataSet) o;
        return uuid == singleDataSet.uuid;
    }
}
