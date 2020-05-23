package com.baqend.core.load.workload;

import java.util.ArrayList;

public class Workload {
    private ArrayList<WorkloadEvent> workload = new ArrayList<WorkloadEvent>();

    public Workload() {
    }

    public Workload(ArrayList<WorkloadEvent> workload) {
        this.workload = workload;
    }

    public ArrayList<WorkloadEvent> getWorkload() {
        return workload;
    }

    public void setWorkload(ArrayList<WorkloadEvent> workload) {
        this.workload = workload;
    }

    public void addWorkloadEvent(WorkloadEvent workloadEvent) {
        workload.add(workloadEvent);
    }
}
