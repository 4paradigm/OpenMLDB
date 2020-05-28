package com._4paradigm.rtidb.client;

public class UpdateResult {
    private boolean success;
    private int affectedCount;

    public UpdateResult(boolean success, int count) {
        this.success = success;
        this.affectedCount = count;
    }

    public UpdateResult(boolean success) {
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getAffectedCount() {
        return affectedCount;
    }
}
