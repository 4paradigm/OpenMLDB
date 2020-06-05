package com._4paradigm.rtidb.client;

public class WriteOption {

    private boolean updateIfExist = false;

    public WriteOption() {
    }

    public WriteOption(boolean updateIfExist, boolean updateIfEqual) {
        this.updateIfExist = updateIfExist;
    }

    public boolean isUpdateIfExist() {
        return updateIfExist;
    }

    public void setUpdateIfExist(boolean updateIfExist) {
        this.updateIfExist = updateIfExist;
    }
}
