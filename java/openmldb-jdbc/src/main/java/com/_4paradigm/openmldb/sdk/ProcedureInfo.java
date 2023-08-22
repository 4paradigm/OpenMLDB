/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.sdk;

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

    private List<String> inputDbs= new ArrayList<>();

    private int routerCol = -1;

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

    public List<String> getInputDbs() {
        return inputDbs;
    }

    public void setInputDbs(List<String> dbs) {
        this.inputDbs = dbs;
    }

    public String getMainTable() {
        return mainTable;
    }

    public void setMainTable(String mainTable) {
        this.mainTable = mainTable;
    }

    public void setRouterCol(int routerCol) {
        this.routerCol = routerCol;
    }

    public int getRouterCol() {
        return routerCol;
    }
}
