package com.unwrittendfs.simulator.client.workload;

import com.unwrittendfs.simulator.dfs.DistributedFileSystem;
import com.unwrittendfs.simulator.exceptions.GenericException;

public class WorkloadFactory {
    public IClientWorkload getWorkload(String workloadType, DistributedFileSystem mDfs) throws GenericException {
        if(WorkloadType.TEST.name().equals(workloadType)){
            return new TestWorkload(mDfs);
        } else if (WorkloadType.SGD.name().equals(workloadType)) {
            return new DownpourSGD(mDfs);
        } else if (WorkloadType.HOT_N_COLD.name().equals(workloadType)) {
            return new HotNCold(mDfs);
        } else {
            throw new GenericException("Invalid workload type");
        }
    }
}
