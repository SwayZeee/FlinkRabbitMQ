package com.baqend.workload;

import java.util.ArrayList;

public class LoadData {
    private final ArrayList<SingleDataSet> load;

    public LoadData() {
        this.load = new ArrayList<SingleDataSet>();
    }

    public ArrayList<SingleDataSet> getLoad() {
        return load;
    }

    public void addSingleDataSet(SingleDataSet singleDataSet) {
        this.load.add(singleDataSet);
    }
}
