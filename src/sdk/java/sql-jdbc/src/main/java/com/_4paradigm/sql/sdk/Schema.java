package com._4paradigm.sql.sdk;

import java.util.ArrayList;
import java.util.List;

public class Schema {
    private List<Column> columnList = new ArrayList<>();

    public Schema(List<Column> columnList) {
        this.columnList = columnList;
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }
}

