package com._4paradigm.sql.sdk;

import java.util.ArrayList;
import java.util.List;

public class ProcedureInfo {
    private String dbName;
    private String proName;
    private String sql;
    private Schema inputSchema;
    private Schema outputSchema;
    private String mainTable;
    private List<String> inputTables = new ArrayList<>();

    public ProcedureInfo() {
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getProName() {
        return proName;
    }

    public void setProName(String proName) {
        this.proName = proName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Schema getInputSchema() {
        return inputSchema;
    }

    public void setInputSchema(Schema inputSchema) {
        this.inputSchema = inputSchema;
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }

    public void setOutputSchema(Schema outputSchema) {
        this.outputSchema = outputSchema;
    }

    public List<String> getInputTables() {
        return inputTables;
    }

    public void setInputTables(List<String> inputTables) {
        this.inputTables = inputTables;
    }

    public String getMainTable() {
        return mainTable;
    }

    public void setMainTable(String mainTable) {
        this.mainTable = mainTable;
    }
}
