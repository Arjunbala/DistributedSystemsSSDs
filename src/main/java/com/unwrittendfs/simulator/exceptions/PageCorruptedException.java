package com.unwrittendfs.simulator.exceptions;

public class PageCorruptedException extends Exception {

    public PageCorruptedException(String message) {
        super(message);
    }

    public PageCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
