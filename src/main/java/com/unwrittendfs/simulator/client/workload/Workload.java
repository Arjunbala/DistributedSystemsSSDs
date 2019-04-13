package com.unwrittendfs.simulator.client.workload;

import java.util.Map;

public class Workload {
    private enum WorkloadType{
        OPEN,
        CREATE,
        READ,
        WRITE,
        SEEK,
        DELETE,
        STAT
    }

    // TODO : Workload Id for logging purpose, We can create and assign workloadId in the code also.
    private Integer workloadId;

    // Helps to figure out the workload type. Depending on the workloadType, we will have key, value pair in the map.
    // It saves us from creating different POJO for different Workload Type.
    private WorkloadType workloadType;

    // Provide flexibility to store any kind of data related to workload
    private Map<String, Object> workloadData;

    public Integer getWorkloadId() {
        return workloadId;
    }

    public void setWorkloadId(Integer workloadId) {
        this.workloadId = workloadId;
    }


    public WorkloadType getWorkloadType() {
        return workloadType;
    }

    public void setWorkloadType(WorkloadType workloadType) {
        this.workloadType = workloadType;
    }

    public Map<String, Object> getWorkloadData() {
        return workloadData;
    }

    public void setWorkloadData(Map<String, Object> workloadData) {
        this.workloadData = workloadData;
    }
}
