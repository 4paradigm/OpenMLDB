package com._4paradigm.sql.sdk;

import java.util.ArrayList;
import java.util.List;

public class ProcedureInfo {
    private String dbName;
    private String proName;
    private String sql;
    private Schema inputSchema;
    private Schema outputSchema;
    private List<String> inputTables = new ArrayList<>();
    private List<String> rtidbTables = new ArrayList<>();

    public ProcedureInfo() {
    }

    public ProcedureInfo(String dbName, String proName, String sql, Schema inputSchema, Schema outputSchema) {
        this.dbName = dbName;
        this.proName = proName;
        this.sql = sql;
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
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

    public List<String> getRtidbTables() {
        return rtidbTables;
    }

    public void setRtidbTables(List<String> rtidbTables) {
        this.rtidbTables = rtidbTables;
    }
}
