package com.unwrittendfs.simulator.exceptions;

import com.unwrittendfs.simulator.dfs.DistributedFileSystem;

public class GenericException extends RuntimeException{

    public GenericException(String message, DistributedFileSystem dfs) {
        super(message);
        dfs.printStats();

    }

    public GenericException(String message, Throwable throwable, DistributedFileSystem dfs) {
        super(message, throwable);
        dfs.printStats();

    }
}
