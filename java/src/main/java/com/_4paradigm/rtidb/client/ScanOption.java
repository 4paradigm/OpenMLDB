package com._4paradigm.rtidb.client;

public class ScanOption {
    private int limit = 0;
    private boolean removeDuplicateRecordByTime = false;
    private int atLeast = 0;
    private String tsName;
    private String idxName;

    public String getIdxName() {
        return idxName;
    }

    public void setIdxName(String idxName) {
        this.idxName = idxName;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public boolean isRemoveDuplicateRecordByTime() {
        return removeDuplicateRecordByTime;
    }

    public void setRemoveDuplicateRecordByTime(boolean removeDuplicateRecordByTime) {
        this.removeDuplicateRecordByTime = removeDuplicateRecordByTime;
    }

    public int getAtLeast() {
        return atLeast;
    }

    public void setAtLeast(int atLeast) {
        this.atLeast = atLeast;
    }

    public String getTsName() {
        return tsName;
    }

    public void setTsName(String tsName) {
        this.tsName = tsName;
    }
}
