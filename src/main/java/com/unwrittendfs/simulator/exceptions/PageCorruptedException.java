package com.unwrittendfs.simulator.exceptions;

import com.unwrittendfs.simulator.dfs.DistributedFileSystem;

public class PageCorruptedException extends RuntimeException {

    public PageCorruptedException(String message, DistributedFileSystem dfs) {
        super(message);
        dfs.printStats();
    }

    public PageCorruptedException(String message, Throwable cause, DistributedFileSystem dfs) {
        super(message, cause);
        dfs.printStats();
    }
}
