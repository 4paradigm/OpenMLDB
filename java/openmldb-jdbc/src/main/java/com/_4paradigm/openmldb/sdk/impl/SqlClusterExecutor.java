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

package com._4paradigm.openmldb.sdk.impl;

import com._4paradigm.openmldb.ColumnDescPair;
import com._4paradigm.openmldb.ColumnDescVector;
import com._4paradigm.openmldb.ExplainInfo;
import com._4paradigm.openmldb.ResultSet;
import com._4paradigm.openmldb.SQLInsertRow;
import com._4paradigm.openmldb.SQLInsertRows;
import com._4paradigm.openmldb.SQLRequestRow;
import com._4paradigm.openmldb.SQLRouter;
import com._4paradigm.openmldb.SQLRouterOptions;
import com._4paradigm.openmldb.StandaloneOptions;
import com._4paradigm.openmldb.Status;
import com._4paradigm.openmldb.TableColumnDescPair;
import com._4paradigm.openmldb.TableColumnDescPairVector;
import com._4paradigm.openmldb.DBTableColumnDescPair;
import com._4paradigm.openmldb.DBTableColumnDescPairVector;
import com._4paradigm.openmldb.DBTableVector;
import com._4paradigm.openmldb.TableReader;
import com._4paradigm.openmldb.VectorString;
import com._4paradigm.openmldb.common.LibraryLoader;
import com._4paradigm.openmldb.common.Pair;
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Common;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sql_router_sdk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SqlClusterExecutor implements SqlExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SqlClusterExecutor.class);

    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private SQLRouter sqlRouter;

    public SqlClusterExecutor(SdkOption option, String libraryPath) throws SqlException {
        initJavaSdkLibrary(libraryPath);

        if (option.isClusterMode()) {
            SQLRouterOptions sqlOpt = option.buildSQLRouterOptions();
            this.sqlRouter = sql_router_sdk.NewClusterSQLRouter(sqlOpt);
            sqlOpt.delete();
        } else {
            StandaloneOptions sqlOpt = option.buildStandaloneOptions();
            this.sqlRouter = sql_router_sdk.NewStandaloneSQLRouter(sqlOpt);
            sqlOpt.delete();
        }
        if (sqlRouter == null) {
            throw new SqlException("fail to create sql executor");
        }
    }

    public SqlClusterExecutor(SdkOption option) throws SqlException {
        this(option, "sql_jsdk");
    }

    synchronized public static void initJavaSdkLibrary(String libraryPath) {
        if (!initialized.get()) {
            if (libraryPath == null || libraryPath.isEmpty()) {
                LibraryLoader.loadLibrary("sql_jsdk");
            } else {
                LibraryLoader.loadLibrary(libraryPath);
            }
            initialized.set(true);
        }
    }

    @Override
    public boolean executeDDL(String db, String sql) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteDDL(db, sql, status);
        if (ok) {
            sqlRouter.RefreshCatalog();
        } else {
            logger.error("executeDDL failed: {}", status.ToString());
        }
        status.delete();
        return ok;
    }

    @Override
    public boolean executeInsert(String db, String sql) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteInsert(db, sql, status);
        if (!ok) {
            logger.error("executeInsert failed: {}", status.ToString());
        }
        status.delete();
        return ok;
    }

    @Override
    public boolean executeInsert(String db, String sql, SQLInsertRow row) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteInsert(db, sql, row, status);
        if (!ok) {
            logger.error("executeInsert failed: {}", status.ToString());
        }
        status.delete();
        return ok;
    }

    @Override
    public boolean executeInsert(String db, String sql, SQLInsertRows rows) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteInsert(db, sql, rows, status);
        if (!ok) {
            logger.error("executeInsert failed: {}", status.ToString());
        }
        status.delete();
        return ok;
    }

    @Override
    public java.sql.ResultSet executeSQL(String db, String sql) {
        Status status = new Status();
        ResultSet rs = sqlRouter.ExecuteSQL(db, sql, status);
        if (status.getCode() != 0) {
            logger.error("executeSQL failed: {}", status.ToString());
        }
        status.delete();
        return new SQLResultSet(rs);
    }

    @Override
    public SQLInsertRow getInsertRow(String db, String sql) {
        Status status = new Status();
        SQLInsertRow row = sqlRouter.GetInsertRow(db, sql, status);
        if (status.getCode() != 0) {
            logger.error("getInsertRow failed: {}", status.ToString());
        }
        status.delete();
        return row;
    }

    @Override
    public Statement getStatement() {
        return new com._4paradigm.openmldb.jdbc.Statement(sqlRouter);
    }

    @Override
    public PreparedStatement getInsertPreparedStmt(String db, String sql) throws SQLException {
        return new InsertPreparedStatementImpl(db, sql, this.sqlRouter);
    }

    @Override
    public PreparedStatement getDeletePreparedStmt(String db, String sql) throws SQLException {
        return new DeletePreparedStatementImpl(db, sql, this.sqlRouter);
    }

    @Override
    public PreparedStatement getRequestPreparedStmt(String db, String sql) throws SQLException {
        return new RequestPreparedStatementImpl(db, sql, this.sqlRouter);
    }

    @Override
    public PreparedStatement getPreparedStatement(String db, String sql) throws SQLException {
        return new PreparedStatementImpl(db, sql, this.sqlRouter);
    }

    @Override
    public PreparedStatement getBatchRequestPreparedStmt(String db, String sql,
            List<Integer> commonColumnIndices) throws SQLException {
        return new BatchRequestPreparedStatementImpl(
                db, sql, this.sqlRouter, commonColumnIndices);
    }

    @Override
    public CallablePreparedStatement getCallablePreparedStmt(String db, String deploymentName) throws SQLException {
        return new CallablePreparedStatementImpl(db, deploymentName, this.sqlRouter);
    }

    @Override
    public CallablePreparedStatement getCallablePreparedStmtBatch(String db, String deploymentName)
            throws SQLException {
        return new BatchCallablePreparedStatementImpl(db, deploymentName, this.sqlRouter);
    }

    @Override
    public SQLInsertRows getInsertRows(String db, String sql) {
        Status status = new Status();
        SQLInsertRows rows = sqlRouter.GetInsertRows(db, sql, status);
        if (status.getCode() != 0) {
            logger.error("getInsertRows failed: {}", status.ToString());
        }
        status.delete();
        return rows;
    }

    @Override
    public ResultSet executeSQLRequest(String db, String sql, SQLRequestRow row) {
        // TODO(wangtaize) add execption
        Status status = new Status();
        ResultSet rs = sqlRouter.ExecuteSQLRequest(db, sql, row, status);
        if (status.getCode() != 0) {
            logger.error("executeSQLRequest failed: {}", status.ToString());
        }
        status.delete();
        return rs;
    }

    @Override
    public Schema getInputSchema(String dbName, String sql) throws SQLException {
        Status status = new Status();
        ExplainInfo explain = sqlRouter.Explain(dbName, sql, status);
        if (status.getCode() != 0 || explain == null) {
            String msg = status.ToString();
            status.delete();
            if (explain != null) {
                explain.delete();
            }
            throw new SQLException("getInputSchema failed: " + msg);
        }
        status.delete();
        List<Column> columnList = new ArrayList<>();
        com._4paradigm.openmldb.Schema schema = explain.GetInputSchema();
        for (int i = 0; i < schema.GetColumnCnt(); i++) {
            Column column = new Column();
            column.setColumnName(schema.GetColumnName(i));
            column.setSqlType(Common.type2SqlType(schema.GetColumnType(i)));
            column.setNotNull(schema.IsColumnNotNull(i));
            column.setConstant(schema.IsConstant(i));
            columnList.add(column);
        }
        schema.delete();
        explain.delete();
        return new Schema(columnList);
    }

    @Override
    public Schema getTableSchema(String dbName, String tableName) throws SQLException {
        com._4paradigm.openmldb.Schema schema = sqlRouter.GetTableSchema(dbName, tableName);
        if (schema == null) {
            throw new SQLException(String.format("table %s not found in db %s", tableName, dbName));
        }

        Schema ret = Common.convertSchema(schema);
        schema.delete();
        return ret;
    }

    @Override
    public com._4paradigm.openmldb.sdk.ProcedureInfo showProcedure(String dbName, String proName) throws SQLException {
        Status status = new Status();
        com._4paradigm.openmldb.ProcedureInfo procedureInfo = sqlRouter.ShowProcedure(dbName, proName, status);
        if (procedureInfo == null || status.getCode() != 0) {
            String msg = status.ToString();
            status.delete();
            if (procedureInfo != null) {
                procedureInfo.delete();
            }
            throw new SQLException("ShowProcedure failed: " + msg);
        }
        status.delete();
        com._4paradigm.openmldb.sdk.ProcedureInfo spInfo = new com._4paradigm.openmldb.sdk.ProcedureInfo();
        spInfo.setDbName(procedureInfo.GetDbName());
        spInfo.setProName(procedureInfo.GetSpName());
        spInfo.setSql(procedureInfo.GetSql());
        spInfo.setInputSchema(Common.convertSchema(procedureInfo.GetInputSchema()));
        spInfo.setOutputSchema(Common.convertSchema(procedureInfo.GetOutputSchema()));
        spInfo.setMainTable(procedureInfo.GetMainTable());
        spInfo.setInputTables(procedureInfo.GetTables());
        spInfo.setInputDbs(procedureInfo.GetDbs());
        procedureInfo.delete();
        return spInfo;
    }

    private static TableColumnDescPairVector convertSchema(Map<String, Schema> tables) throws SQLException {
        TableColumnDescPairVector result = new TableColumnDescPairVector();
        for (Map.Entry<String, Schema> entry : tables.entrySet()) {
            String table = entry.getKey();
            Schema schema = entry.getValue();
            ColumnDescVector columnDescVector = new ColumnDescVector();
            List<Column> columnList = schema.getColumnList();
            for (Column column : columnList) {
                String columnName = column.getColumnName();
                int sqlType = column.getSqlType();
                columnDescVector.add(new ColumnDescPair(columnName, Common.sqlTypeToDataType(sqlType)));
            }
            result.add(new TableColumnDescPair(table, columnDescVector));
        }
        return result;
    }

    public static List<String> genDDL(String sql, Map<String, Map<String, Schema>> tableSchema)
            throws SQLException {
        SqlClusterExecutor.initJavaSdkLibrary("");

        if (null == tableSchema || tableSchema.isEmpty()) {
            throw new SQLException("input schema is null or empty");
        }
        List<String> results = new ArrayList<>();
        // TODO(hw): multi db is not supported now, create db.table works?
        for (Map.Entry<String, Map<String, Schema>> entry : tableSchema.entrySet()) {
            Map<String, Schema> schemaMap = entry.getValue();
            TableColumnDescPairVector tableColumnDescPairVector = convertSchema(schemaMap);
            VectorString ddlList = sql_router_sdk.GenDDL(sql, tableColumnDescPairVector);
            results.addAll(ddlList); // hard copy
            ddlList.delete();
            tableColumnDescPairVector.delete();
        }
        return results;
    }

    public static Schema genOutputSchema(String sql, String usedDB, Map<String, Map<String, Schema>> tableSchema)
            throws SQLException {
        SqlClusterExecutor.initJavaSdkLibrary("");

        if (null == tableSchema || tableSchema.isEmpty()) {
            throw new SQLException("input schema is null or empty");
        }
        DBTableColumnDescPairVector dbTableColumnDescPairVector = new DBTableColumnDescPairVector();
        // com._4paradigm.openmldb.Schema has no ctor, so we use a new struct
        // map -> vector, for swig
        for (Map.Entry<String, Map<String, Schema>> entry : tableSchema.entrySet()) {
            String db = entry.getKey();
            Map<String, Schema> schemaMap = entry.getValue();
            dbTableColumnDescPairVector.add(new DBTableColumnDescPair(db, convertSchema(schemaMap)));
        }
        com._4paradigm.openmldb.Schema outputSchema = sql_router_sdk.GenOutputSchema(sql, usedDB,
                dbTableColumnDescPairVector);
        // TODO(hw): if we convert com._4paradigm.openmldb.Schema(cPtr) failed, it will
        // throw an exception, we can't do the later delete()
        Schema ret = Common.convertSchema(outputSchema);
        outputSchema.delete();
        dbTableColumnDescPairVector.delete();
        return ret;
    }

    // for back compatibility, genOutputSchema can set no usedDB, just use the first db in tableSchema
    public static Schema genOutputSchema(String sql, Map<String, Map<String, Schema>> tableSchema)
            throws SQLException {
        return genOutputSchema(sql, tableSchema.keySet().iterator().next(), tableSchema);
    }

    // NOTICE: even tableSchema is <db, <table, schema>>, we'll assume that all
    // tables in one db in sql_router_sdk
    // returns
    // 1. empty list: means valid
    // 2. otherwise a list(len 2):[0] the error msg; [1] the trace
    public static List<String> validateSQLInBatch(String sql, String usedDB,
            Map<String, Map<String, Schema>> tableSchema) throws SQLException {
        SqlClusterExecutor.initJavaSdkLibrary("");

        if (null == tableSchema || tableSchema.isEmpty()) {
            throw new SQLException("input schema is null or empty");
        }
        DBTableColumnDescPairVector dbTableColumnDescPairVector = new DBTableColumnDescPairVector();
        for (Map.Entry<String, Map<String, Schema>> entry : tableSchema.entrySet()) {
            String db = entry.getKey();
            Map<String, Schema> schemaMap = entry.getValue();
            dbTableColumnDescPairVector.add(new DBTableColumnDescPair(db, convertSchema(schemaMap)));
        }
        List<String> err = sql_router_sdk.ValidateSQLInBatch(sql, usedDB, dbTableColumnDescPairVector);
        dbTableColumnDescPairVector.delete();
        return err;
    }

    // for back compatibility, validateSQLInBatch can set no usedDB, just use the first db in tableSchema
    // so if only one db, sql can be <table>, otherwise sql must be <db>.<table>
    public static List<String> validateSQLInBatch(String sql, Map<String, Map<String, Schema>> tableSchema)
            throws SQLException {
        return validateSQLInBatch(sql, tableSchema.keySet().iterator().next(), tableSchema);
    }

    // return: the same as validateSQLInBatch
    public static List<String> validateSQLInRequest(String sql, String usedDB,
            Map<String, Map<String, Schema>> tableSchema)
            throws SQLException {
        SqlClusterExecutor.initJavaSdkLibrary("");

        if (null == tableSchema || tableSchema.isEmpty()) {
            throw new SQLException("input schema is null or empty");
        }
        DBTableColumnDescPairVector dbTableColumnDescPairVector = new DBTableColumnDescPairVector();
        for (Map.Entry<String, Map<String, Schema>> entry : tableSchema.entrySet()) {
            String db = entry.getKey();
            Map<String, Schema> schemaMap = entry.getValue();
            dbTableColumnDescPairVector.add(new DBTableColumnDescPair(db, convertSchema(schemaMap)));
        }
        List<String> err = sql_router_sdk.ValidateSQLInRequest(sql, usedDB, dbTableColumnDescPairVector);
        dbTableColumnDescPairVector.delete();
        return err;
    }

    // for back compatibility, validateSQLInRequest can set no usedDB, just use the first db in tableSchema
    public static List<String> validateSQLInRequest(String sql, Map<String, Map<String, Schema>> tableSchema)
            throws SQLException {
        return validateSQLInRequest(sql, tableSchema.keySet().iterator().next(), tableSchema);
    }

    // list<db,table>, the first table is main table, [1, end) are dependent
    // tables(no main table)
    // TODO(hw): returned pair db may be empty? seems always not empty
    public static List<Pair<String, String>> getDependentTables(String sql, String usedDB,
            Map<String, Map<String, Schema>> tableSchema)
            throws SQLException {
        SqlClusterExecutor.initJavaSdkLibrary("");

        if (null == tableSchema || tableSchema.isEmpty()) {
            throw new SQLException("input schema is null or empty");
        }
        DBTableColumnDescPairVector dbTableColumnDescPairVector = new DBTableColumnDescPairVector();
        for (Map.Entry<String, Map<String, Schema>> entry : tableSchema.entrySet()) {
            String db = entry.getKey();
            Map<String, Schema> schemaMap = entry.getValue();
            dbTableColumnDescPairVector.add(new DBTableColumnDescPair(db, convertSchema(schemaMap)));
        }
        DBTableVector tables = sql_router_sdk.GetDependentTables(sql, usedDB, dbTableColumnDescPairVector);
        List<Pair<String, String>> ret = new ArrayList<>();
        for (int i = 0; i < tables.size(); i++) {
            ret.add(new Pair<>(tables.get(i).getFirst(), tables.get(i).getSecond()));
        }
        tables.delete();
        dbTableColumnDescPairVector.delete();
        return ret;
    }

    // literal merge, and validated by validateSQLInRequest, notice that mainTable
    // may not in usedDB
    // return: select * from ..., including the join keys
    private static String mergeSQLWithPartValidate(List<String> sqls, String usedDB, String mainTable,
            List<String> uniqueKeys,
            Map<String, Map<String, Schema>> tableSchema) throws SQLException {
        // make uniqueKeys more unique, gen keys selection
        List<String> uniqueKeysNewName = new ArrayList<>();
        List<String> uniqueKeysRenamed = new ArrayList<>(); // always db.table.key
        for (int i = 0; i < uniqueKeys.size(); i++) {
            uniqueKeysNewName.add(String.format("merge_%s_", uniqueKeys.get(i)));
            // <mainTable>.<key> as merge_<key>_?, use <mainTable>.<key> to avoid ambiguous
            // column with other tables
            uniqueKeysRenamed.add(String.format("%s.%s as merge_%s_", mainTable, uniqueKeys.get(i), uniqueKeys.get(i)));
        }
        List<String> outParts = new ArrayList<>();
        for (int i = 0; i < sqls.size(); i++) {
            // add unique key to each sql
            String sql = sqls.get(i);
            if (!sql.toLowerCase().startsWith("select ")) {
                throw new IllegalArgumentException("sql must be select");
            }
            // remove the last ';'
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            // (select <keys>, <origin features> from ...) as out<i>
            final int idx = i;
            String outSelection = String.format("select %s,%s",
                    uniqueKeysRenamed.stream().map(keyRenamed -> keyRenamed + idx).collect(Collectors.joining(", ")),
                    sql.substring(6));
            // validate
            List<String> ret = SqlClusterExecutor.validateSQLInRequest(outSelection, usedDB, tableSchema);
            if (!ret.isEmpty()) {
                throw new SQLException("sql with uniquekeys [" + outSelection + "] is invalid: " + ret);
            }
            outParts.add(String.format("(%s) as out%d", outSelection, i));
        }
        // last join all parts
        StringBuilder sb = new StringBuilder();
        sb.append("select * from ");
        for (int i = 0; i < outParts.size(); i++) {
            if (i > 0) {
                sb.append(" last join ");
            }
            sb.append(outParts.get(i));
            if (i > 0) {
                // on out0.<key> = out1.<key> and out0.<key> = out2.<key> ...
                sb.append(" on ");
                String equalPattern = "out0.%s0 = out%d.%s%d";
                final int idx = i;
                sb.append(uniqueKeysNewName.stream()
                        .map(newNamePrefix -> String.format(equalPattern, newNamePrefix, idx, newNamePrefix, idx))
                        .collect(Collectors.joining(" and ")));
            }
        }
        sb.append(";");
        return sb.toString();
    }

    // input: sqls, using db, unique keys in main table, table schema
    // with table schema, we can except join keys and validate the merged sql
    public static String mergeSQL(List<String> sqls, String db, List<String> uniqueKeys,
            Map<String, Map<String, Schema>> tableSchema)
            throws SQLException {
        // ensure each sql is valid
        for (String sql : sqls) {
            List<String> ret = SqlClusterExecutor.validateSQLInRequest(sql, db,
                    tableSchema);
            if (!ret.isEmpty()) {
                throw new SQLException("sql is invalid, can't merge: " + ret + ", sql: " + sql);
            }
        }

        // get main table from first sql
        Pair<String, String> mainTablePr = SqlClusterExecutor.getDependentTables(sqls.get(0), db, tableSchema).get(0);
        String mainTable = mainTablePr.getKey().isEmpty() ? db : mainTablePr.getKey() + "." + mainTablePr.getValue();
        // main table is used to combine unique keys, to avoid some ambiguous column
        // name problem, whatever the main table pattern in sql(even renamed),
        // <db>.<table>.<key> as merge_<key>_ always works
        String merged = mergeSQLWithPartValidate(sqls, db, mainTable, uniqueKeys, tableSchema);

        // try to do column filter, if failed, we'll throw an exception
        Schema outputSchema = SqlClusterExecutor.genOutputSchema(merged, tableSchema);
        List<String> cols = outputSchema.getColumnList().stream().map(c -> c.getColumnName())
                .collect(Collectors.toList());
        if (!cols.stream().allMatch(new HashSet<>()::add)) {
            throw new SQLException(
                    "output schema contains ambiguous column name, can't do column filter. please use alias " + cols);
        }
        // TODO(hw): merge_ is unique enough?
        String filtered = "select " + cols.stream().filter(name -> !name.startsWith("merge_"))
                .collect(Collectors.joining("`, `", "`", "`")) + merged.substring(8);
        merged = filtered;

        // validate
        List<String> ret = SqlClusterExecutor.validateSQLInRequest(merged, db, tableSchema);
        if (!ret.isEmpty()) {
            throw new SQLException("merged sql is invalid: " + ret + ", merged sql: " + merged);
        }
        return merged;
    }

    @Override
    public boolean createDB(String db) {
        Status status = new Status();
        boolean ok = sqlRouter.CreateDB(db, status);
        if (status.getCode() != 0) {
            logger.error("create db failed: {}", status.ToString());
        }
        status.delete();
        return ok;
    }

    @Override
    public TableReader getTableReader() {
        return sqlRouter.GetTableReader();
    }

    @Override
    public boolean dropDB(String db) {
        Status status = new Status();
        boolean ok = sqlRouter.DropDB(db, status);
        if (status.getCode() != 0) {
            logger.error("drop db failed: {}", status.ToString());
        }
        status.delete();
        return ok;
    }

    @Override
    public void close() {
        if (sqlRouter != null) {
            sqlRouter.delete();
            sqlRouter = null;
        }
    }

    public List<String> showDatabases() {
        List<String> databases = new ArrayList<>();

        Status status = new Status();
        VectorString dbs = new VectorString();
        boolean ok = sqlRouter.ShowDB(dbs, status);
        if (!ok) {
            logger.error("showDatabases failed: {}", status.ToString());
        } else {
            databases.addAll(dbs);
        }

        status.delete();
        dbs.delete();
        return databases;
    }

    @Override
    public List<String> getTableNames(String db) {
        VectorString names = sqlRouter.GetTableNames(db);
        List<String> tableNames = new ArrayList<>(names);
        names.delete();
        return tableNames;
    }

    public NS.TableInfo getTableInfo(String db, String table) {
        return sqlRouter.GetTableInfo(db, table);
    }

    public boolean updateOfflineTableInfo(NS.TableInfo info) {
        return sqlRouter.UpdateOfflineTableInfo(info);
    }

    public boolean refreshCatalog() {
        return sqlRouter.RefreshCatalog();
    }
}
