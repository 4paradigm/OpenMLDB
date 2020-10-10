package com._4paradigm.sql.sdk;

import java.util.ArrayList;
import java.util.List;

public class Schema {
    private String tableName;
    private List<Column> columnList = new ArrayList<>();

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

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

