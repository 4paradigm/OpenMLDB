package com._4paradigm.fesql.flink.common;

public class FesqlException extends Exception {

    private String message;
    private Throwable cause;

    public FesqlException(String message) {
        this.message = message;
        this.cause = null;
    }

    public FesqlException(String message, Throwable cause) {
        this.message = message;
        this.cause = cause;
    }

    public synchronized Throwable getCause() {
        return this.cause;
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
