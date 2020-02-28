package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.tablet.Tablet;

public class ReadFilter {

    private String colName;
    private Tablet.GetType getType = Tablet.GetType.kSubKeyEq;
    private Object value;

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public Tablet.GetType getGetType() {
        return getType;
    }

    public void setGetType(Tablet.GetType getType) {
        this.getType = getType;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
