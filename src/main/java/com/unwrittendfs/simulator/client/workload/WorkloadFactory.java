package com.unwrittendfs.simulator.client.workload;

import com.unwrittendfs.simulator.dfs.DistributedFileSystem;

public class WorkloadFactory {
    public IClientWorkload getWorkload(String workloadType, DistributedFileSystem mDfs){
        if(WorkloadType.TEST.name().equals(workloadType)){
            return new TestWorkload(mDfs);
        } else if(WorkloadType.SGD.name().equals(workloadType)){
            return new DownPourSGD(mDfs);
        } else {
            throw new RuntimeException("Invalid workload type");
        }
    }
}
