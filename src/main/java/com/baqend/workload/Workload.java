package com.baqend.workload;

import java.util.ArrayList;

public class Workload {
    private final ArrayList<WorkloadEvent> workload = new ArrayList<WorkloadEvent>();

    public Workload() {
    }

    public ArrayList<WorkloadEvent> getWorkload() {
        return workload;
    }

    public void addWorkloadEvent(WorkloadEvent workloadEvent) {
        workload.add(workloadEvent);
    }
}
