package com._4paradigm.sql.sdk;

public class ProcedureInfo {
    private String proName;
    private String proSql;
    private Schema inputSchema;
    private Schema outputSchema;

    public String getProName() {
        return proName;
    }

    public void setProName(String proName) {
        this.proName = proName;
    }

    public String getProSql() {
        return proSql;
    }

    public void setProSql(String proSql) {
        this.proSql = proSql;
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
}
