package com._4paradigm.rtidb.client;

public class PutResult {
    private boolean success;
    private long autoGenPk;
    private boolean hasAutoGenPk;

    public PutResult(boolean success, long autoGenPk) {
        this.success = success;
        this.autoGenPk = autoGenPk;
        this.hasAutoGenPk = true;
    }

    public PutResult(boolean success) {
        this.success = success;
        this.hasAutoGenPk = false;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getAutoGenPk() throws TabletException {
        if (!hasAutoGenPk) {
            throw new TabletException("no autoGenPk!");
        }
        return autoGenPk;
    }

    public boolean hasAutoGenPk() {
        return hasAutoGenPk;
    }
}
