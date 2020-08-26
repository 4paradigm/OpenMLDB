package com._4paradigm.fesql.common;

public class UnsupportedFesqlException extends Exception {

    private String message;
    private Throwable cause;

    public UnsupportedFesqlException(String message) {
        this.message = message;
        this.cause = null;
    }

    public UnsupportedFesqlException(String message, Throwable cause) {
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
