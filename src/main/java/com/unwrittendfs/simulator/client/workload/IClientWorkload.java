package com.unwrittendfs.simulator.client.workload;

import com.unwrittendfs.simulator.exceptions.GenericException;
import com.unwrittendfs.simulator.exceptions.PageCorruptedException;

public interface IClientWorkload {
    public void execute() throws GenericException, PageCorruptedException;
}
