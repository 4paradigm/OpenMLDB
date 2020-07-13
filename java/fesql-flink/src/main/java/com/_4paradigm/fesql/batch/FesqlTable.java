package com._4paradigm.fesql.batch;

import org.apache.flink.table.api.*;
import org.apache.flink.table.operations.QueryOperation;

public class FesqlTable {

    private Table table;

    public FesqlTable(Table table) {
        this.table = table;
    }

    public TableSchema getSchema() {
        return this.table.getSchema();
    }

    public void printSchema() {
        this.table.printSchema();
    }

    public QueryOperation getQueryOperation() {
        return this.table.getQueryOperation();
    }

    public TableResult executeInsert(String tablePath) {
        return this.table.executeInsert(tablePath);
    }

    public TableResult executeInsert(String tablePath, boolean overwrite) {
        return this.table.executeInsert(tablePath, overwrite);
    }

    public TableResult execute() {
        return this.table.execute();
    }

    public String explain(ExplainDetail... explainDetails) {
        return this.table.explain(explainDetails);
    }

}
