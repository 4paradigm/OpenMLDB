package com._4paradigm.rtidb.client.schema;

public class WriteOption {

    private boolean updateIfExist = true;
    private boolean updateIfEqual = true;

    public WriteOption() {
    }

    public WriteOption(boolean updateIfExist, boolean updateIfEqual) {
        this.updateIfExist = updateIfExist;
        this.updateIfEqual = updateIfEqual;
    }

    public boolean isUpdateIfExist() {
        return updateIfExist;
    }

    public void setUpdateIfExist(boolean updateIfExist) {
        this.updateIfExist = updateIfExist;
    }

    public boolean isUpdateIfEqual() {
        return updateIfEqual;
    }

    public void setUpdateIfEqual(boolean updateIfEqual) {
        this.updateIfEqual = updateIfEqual;
    }
}
