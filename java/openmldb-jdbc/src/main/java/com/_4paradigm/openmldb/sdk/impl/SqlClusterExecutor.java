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
import com._4paradigm.openmldb.common.zk.ZKClient;
import com._4paradigm.openmldb.common.zk.ZKConfig;
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.sdk.*;
import com._4paradigm.openmldb.sql_router_sdk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SqlClusterExecutor implements SqlExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SqlClusterExecutor.class);

    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private SQLRouter sqlRouter;
    private DeploymentManager deploymentManager;
    private InsertPreparedStatementCache insertCache;

    public SqlClusterExecutor(SdkOption option, String libraryPath) throws SqlException {
        initJavaSdkLibrary(libraryPath);
        ZKClient zkClient = null;
        if (option.isClusterMode()) {
            SQLRouterOptions sqlOpt = option.buildSQLRouterOptions();
            this.sqlRouter = sql_router_sdk.NewClusterSQLRouter(sqlOpt);
            sqlOpt.delete();
            if (!option.isLight()) {
                zkClient = new ZKClient(ZKConfig.builder()
                        .cluster(option.getZkCluster())
                        .namespace(option.getZkPath())
                        .sessionTimeout((int)option.getSessionTimeout())
                        .build());
                try {
                    if (!zkClient.connect()) {
                        throw new SqlException("zk client connect failed.");
                    }
                } catch (Exception e) {
                    throw new SqlException("init zk client failed", e);
                }
            }
        } else {
            StandaloneOptions sqlOpt = option.buildStandaloneOptions();
            this.sqlRouter = sql_router_sdk.NewStandaloneSQLRouter(sqlOpt);
            sqlOpt.delete();
        }
        if (sqlRouter == null) {
            throw new SqlException("fail to create sql executor");
        }
        deploymentManager = new DeploymentManager(zkClient);
        insertCache = new InsertPreparedStatementCache(option.getMaxSqlCacheSize(), zkClient);
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
        return new NativeResultSet(rs);
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
        InsertPreparedStatementMeta meta = insertCache.get(db, sql);
        if (meta == null) {
            Status status = new Status();
            SQLInsertRow row = sqlRouter.GetInsertRow(db, sql, status);
            if (!status.IsOK()) {
                String msg = status.ToString();
                status.delete();
                if (row != null) {
                    row.delete();
                }
                throw new SQLException("getSQLInsertRow failed, " + msg);
            }
            status.delete();
            String name = row.GetTableInfo().getName();
            NS.TableInfo tableInfo = getTableInfo(db, name);
            meta = new InsertPreparedStatementMeta(sql, tableInfo, row);
            row.delete();
            insertCache.put(db, sql, meta);
        }
        return new InsertPreparedStatementImpl(meta, this.sqlRouter);
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
        Deployment deployment = deploymentManager.getDeployment(db, deploymentName);
        if (deployment == null) {
            try {
                ProcedureInfo procedureInfo = showProcedure(db, deploymentName);
                deployment = new Deployment(procedureInfo);
                deploymentManager.addDeployment(db, deploymentName, deployment);
            } catch (Exception e) {
                throw new SQLException("deployment does not exist. db name " + db + " deployment name " + deploymentName);
            }
        }
        return new CallablePreparedStatementImpl(deployment, this.sqlRouter);
    }

    @Override
    public CallablePreparedStatement getCallablePreparedStmtBatch(String db, String deploymentName)
            throws SQLException {
        Deployment deployment = deploymentManager.getDeployment(db, deploymentName);
        if (deployment == null) {
            try {
                ProcedureInfo procedureInfo = showProcedure(db, deploymentName);
                deployment = new Deployment(procedureInfo);
                deploymentManager.addDeployment(db, deploymentName, deployment);
            } catch (Exception e) {
                throw new SQLException("deployment does not exist. db name " + db + " deployment name " + deploymentName);
            }
        }
        return new BatchCallablePreparedStatementImpl(deployment, this.sqlRouter);
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
        com._4paradigm.openmldb.sdk.ProcedureInfo spInfo = Common.convertProcedureInfo(procedureInfo);
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
        // TODO(hw): multi db is not supported now
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
    public static String mergeSQL(List<String> sqls, String usedDB, List<String> uniqueKeys,
            Map<String, Map<String, Schema>> tableSchema)
            throws SQLException {
        // ensure each sql is valid
        for (String sql : sqls) {
            List<String> ret = SqlClusterExecutor.validateSQLInRequest(sql, usedDB,
                    tableSchema);
            if (!ret.isEmpty()) {
                throw new SQLException("sql is invalid, can't merge: " + ret + ", sql: " + sql);
            }
        }

        // get main table from first sql
        Pair<String, String> mainTablePr = SqlClusterExecutor.getDependentTables(sqls.get(0), usedDB, tableSchema).get(0);
        String mainTable = mainTablePr.getKey().isEmpty() ? usedDB : mainTablePr.getKey() + "." + mainTablePr.getValue();
        // main table is used to combine unique keys, to avoid some ambiguous column
        // name problem, whatever the main table pattern in sql(even renamed),
        // <db>.<table>.<key> as merge_<key>_ always works
        String merged = mergeSQLWithPartValidate(sqls, usedDB, mainTable, uniqueKeys, tableSchema);

        // try to do column filter, if failed, we'll throw an exception
        Schema outputSchema = SqlClusterExecutor.genOutputSchema(merged, usedDB, tableSchema);
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
        List<String> ret = SqlClusterExecutor.validateSQLInRequest(merged, usedDB, tableSchema);
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

    @Override
    public DAGNode SQLToDAG(String query) throws SQLException {
        Status status = new Status();
        final com._4paradigm.openmldb.DAGNode dag = sqlRouter.SQLToDAG(query, status);

        try {
            if (status.getCode() != 0) {
                throw new SQLException(status.ToString());
            }
            return convertDAG(dag);
        } finally {
            dag.delete();
            status.delete();
        }
    }

    private static DAGNode convertDAG(com._4paradigm.openmldb.DAGNode dag) {
        ArrayList<DAGNode> convertedProducers = new ArrayList<>();
        for (com._4paradigm.openmldb.DAGNode producer : dag.getProducers()) {
            final DAGNode converted = convertDAG(producer);
            convertedProducers.add(converted);
        }

        return new DAGNode(dag.getName(), dag.getSql(), convertedProducers);
    }

    static private class AIOSDAGNode {
        public String uuid;
        public String script;
        public ArrayList<String> parents = new ArrayList<>();
        public ArrayList<String> inputTables = new ArrayList<>();
        public Map<String, String> tableNameMap = new HashMap<>();
    }
    
    static private class AIOSDAGColumn {
        public String name;
        public String type;
    }

    static private class AIOSDAGSchema {
        public String prn;
        public List<AIOSDAGColumn> cols = new ArrayList<>();
    }

    static private class AIOSDAG {
        public List<AIOSDAGNode> nodes = new ArrayList<>();
        public List<AIOSDAGSchema> schemas = new ArrayList<>();
    }

    static private class AIOSDAGMerge {
        public String sql;
        public Map<String, Schema> tableSchema = new HashMap<>();
    }

    @Override
    public String mergeAIOSDAGSQL(String query) throws SQLException {
        Gson gson = new Gson();
        AIOSDAG dag = gson.fromJson(query, AIOSDAG.class);
        Queue<String> queue = new LinkedList<>();
        Map<String, Schema> schemaMap = new HashMap<>();
        Map<String, AIOSDAGNode> nodeMap = new HashMap<>();
        Map<String, List<String>> childrenMap = new HashMap<>();
        Map<String, Integer> parentsMap = new HashMap<>();
        Map<String, AIOSDAGMerge> mergeMap = new HashMap<>();
        Map<String, Types> typeMap = new HashMap<>();

        for (AIOSDAGNode node : dag.nodes) {
            if (nodeMap.get(node.uuid) != null) {
                throw new RuntimeException("Duplicate 'uuid': " + node.uuid);
            }
            if (node.parents.size() != node.inputTables.size()) {
                throw new RuntimeException("Size of 'parents' and 'inputTables' mismatch: " + node.uuid);
            }
            nodeMap.put(node.uuid, node);
        }

        for (AIOSDAGSchema schema : dag.schemas) {
            List<Column> columns = new ArrayList<>();
            for (AIOSDAGColumn column : schema.cols) {
                try {
                    int type = Types.class.getField(column.type.toUpperCase()).getInt(null);
                    columns.add(new Column(column.name, type));
                } catch (Exception e) {
                    throw new RuntimeException("Unknown SQL type: " + column.type);
                }
            }
            schemaMap.put(schema.prn, new Schema(columns));
        }

        for (AIOSDAGNode node : dag.nodes) {
            int degree = 0;
            for (int i = 0; i < node.parents.size(); i++) {
                String parent = node.parents.get(i);
                String table = node.inputTables.get(i);
                if (nodeMap.get(parent) != null) {
                    degree += 1;
                    if (childrenMap.get(parent) == null) {
                        childrenMap.put(parent, new ArrayList<>());
                    }
                    childrenMap.get(parent).add(node.uuid);
                }
            }
            parentsMap.put(node.uuid, degree);
            if (degree == 0) {
                queue.offer(node.uuid);
            }
        }

        AIOSDAGMerge result = null;
        while (queue.peek() != null) {
            AIOSDAGNode node =  nodeMap.get(queue.poll());
            AIOSDAGMerge merge = new AIOSDAGMerge();
            StringBuilder sql = new StringBuilder("WITH");
            boolean with = false;
            for (int i = 0; i < node.parents.size(); i++) {
                String parent = node.parents.get(i);
                String table = node.inputTables.get(i);
                AIOSDAGMerge input = mergeMap.get(parent);
                if (input == null) {
                    String prn = node.tableNameMap.get(table);
                    if (prn == null) {
                        throw new RuntimeException("'prn' not found in 'tableNameMap': " +
                            node.uuid + " " + table);
                    }
                    Schema schema = schemaMap.get(prn);
                    if (schema == null) {
                        throw new RuntimeException("schema not found: " + prn);
                    }
                    if (merge.tableSchema.get(table) != null) {
                        if (merge.tableSchema.get(table) != schema) {
                            throw new RuntimeException("table name conflict: " + table);
                        }
                    }
                    merge.tableSchema.put(table, schema);
                } else {
                    if (with) {
                        sql.append(",");
                    }
                    sql.append(" ").append(table).append(" as (\n");
                    sql.append(input.sql).append("\n").append(")");
                    with = true;
                    for (Map.Entry<String, Schema> entry : input.tableSchema.entrySet()) {
                        if (merge.tableSchema.get(entry.getKey()) != null) {
                            if (merge.tableSchema.get(entry.getKey()) != entry.getValue()) {
                                throw new RuntimeException("Table name conflict: " + entry.getKey());
                            }
                        }
                        merge.tableSchema.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            if (with) {
                sql.append("\n").append(node.script).append("\n");
                merge.sql = sql.toString();
            } else {
                merge.sql = node.script;
            }
            mergeMap.put(node.uuid, merge);
            List<String> children = childrenMap.get(node.uuid);
            if (children == null || children.size() == 0) {
                if (result != null) {
                   throw new RuntimeException("Invalid DAG: final node not unique");
                }
                result = merge;
            } else {
                for (String child : children) {
                    parentsMap.put(child, parentsMap.get(child) - 1);
                    if (parentsMap.get(child) == 0) {
                        queue.offer(child);
                    }
                }
            }
        }

        if (result == null) {
            throw new RuntimeException("Invalid DAG: final node not found");
        }

        Map<String, Map<String, Schema>> tableSchema = new HashMap<>();
        tableSchema.put("nousedDB", result.tableSchema);
        List<String> err = validateSQLInRequest(result.sql, tableSchema);
        if (err.size() != 0) {
            throw new SQLException(String.join("\n", err));
        }
        return result.sql;
    }
}
