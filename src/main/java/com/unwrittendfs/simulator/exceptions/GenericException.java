package com.unwrittendfs.simulator.exceptions;

public class GenericException extends Exception {

    public GenericException(String message) {
        super(message);

    }

    public GenericException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
