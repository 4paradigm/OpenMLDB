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
import com._4paradigm.openmldb.Status;
import com._4paradigm.openmldb.TableColumnDescPair;
import com._4paradigm.openmldb.TableColumnDescPairVector;
import com._4paradigm.openmldb.TableReader;
import com._4paradigm.openmldb.VectorString;
import com._4paradigm.openmldb.common.LibraryLoader;
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

import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlClusterExecutor implements SqlExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SqlClusterExecutor.class);

    static {
        initJavaSdkLibrary();
    }

    private static boolean initialized = false;
    private SQLRouter sqlRouter;

    public static void initJavaSdkLibrary() {
        if (!initialized) {
            LibraryLoader.loadLibrary("sql_jsdk");
            initialized = true;
        }
    }

    public SqlClusterExecutor(SdkOption option) throws SqlException {

        SQLRouterOptions sqlOpt = new SQLRouterOptions();
        sqlOpt.setSession_timeout(option.getSessionTimeout());
        sqlOpt.setZk_cluster(option.getZkCluster());
        sqlOpt.setZk_path(option.getZkPath());
        sqlOpt.setEnable_debug(option.getEnableDebug());
        sqlOpt.setRequest_timeout(option.getRequestTimeout());
        this.sqlRouter = sql_router_sdk.NewClusterSQLRouter(sqlOpt);
        sqlOpt.delete();
        if (sqlRouter == null) {
            throw new SqlException("fail to create sql executor");
        }
    }

    public static String findSdkLibraryPath() throws UnsatisfiedLinkError {
        String libraryPath = "sql_jsdk";
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.equals("mac os x")) {
            libraryPath = "lib" + libraryPath + ".dylib";
        } else if (osName.contains("linux")) {
            libraryPath = "lib" + libraryPath + ".so";
        }
        URL resource = LibraryLoader.class.getClassLoader().getResource(libraryPath);
        if (resource == null) {
            throw new UnsatisfiedLinkError(String.format("Fail to load library %s", libraryPath));
        }
        return resource.getPath();
    }

    @Override
    public boolean executeDDL(String db, String sql) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteDDL(db, sql, status);
        if (ok) {
            sqlRouter.RefreshCatalog();
        } else {
            logger.error("executeDDL fail: {}", status.getMsg());
        }
        status.delete();
        return ok;
    }

    @Override
    public boolean executeInsert(String db, String sql) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteInsert(db, sql, status);
        if (!ok) {
            logger.error("executeInsert fail: {}", status.getMsg());
        }
        status.delete();
        return ok;
    }

    @Override
    public boolean executeInsert(String db, String sql, SQLInsertRow row) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteInsert(db, sql, row, status);
        if (!ok) {
            logger.error("executeInsert fail: {}", status.getMsg());
        }
        status.delete();
        return ok;
    }

    @Override
    public boolean executeInsert(String db, String sql, SQLInsertRows rows) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteInsert(db, sql, rows, status);
        if (!ok) {
            logger.error("executeInsert fail: {}", status.getMsg());
        }
        status.delete();
        return ok;
    }

    @Override
    public java.sql.ResultSet executeSQL(String db, String sql) {
        Status status = new Status();
        ResultSet rs = sqlRouter.ExecuteSQL(db, sql, status);
        if (status.getCode() != 0) {
            logger.error("executeSQL fail: {}", status.getMsg());
        }
        status.delete();
        return new SQLResultSet(rs);
    }

    @Override
    public SQLInsertRow getInsertRow(String db, String sql) {
        Status status = new Status();
        SQLInsertRow row = sqlRouter.GetInsertRow(db, sql, status);
        if (status.getCode() != 0) {
            logger.error("getInsertRow fail: {}", status.getMsg());
        }
        status.delete();
        return row;
    }

    public PreparedStatement getInsertPreparedStmt(String db, String sql) throws SQLException {
        return new InsertPreparedStatementImpl(db, sql, this.sqlRouter);
    }

    public PreparedStatement getRequestPreparedStmt(String db, String sql) throws SQLException {
        return new RequestPreparedStatementImpl(db, sql, this.sqlRouter);
    }

    public PreparedStatement getPreparedStatement(String db, String sql) throws SQLException {
        return new PreparedStatementImpl(db, sql, this.sqlRouter);
    }

    public PreparedStatement getBatchRequestPreparedStmt(String db, String sql,
                                                         List<Integer> commonColumnIndices) throws SQLException {
        return new BatchRequestPreparedStatementImpl(
                db, sql, this.sqlRouter, commonColumnIndices);
    }

    public CallablePreparedStatement getCallablePreparedStmt(String db, String spName) throws SQLException {
        return new CallablePreparedStatementImpl(db, spName, this.sqlRouter);
    }

    @Override
    public CallablePreparedStatement getCallablePreparedStmtBatch(String db, String spName) throws SQLException {
        return new BatchCallablePreparedStatementImpl(db, spName, this.sqlRouter);
    }

    @Override
    public SQLInsertRows getInsertRows(String db, String sql) {
        Status status = new Status();
        SQLInsertRows rows = sqlRouter.GetInsertRows(db, sql, status);
        if (status.getCode() != 0) {
            logger.error("getInsertRow fail: {}", status.getMsg());
        }
        status.delete();
        return rows;
    }

    @Override
    public ResultSet executeSQLRequest(String db, String sql, SQLRequestRow row) {
        //TODO(wangtaize) add execption
        Status status = new Status();
        ResultSet rs = sqlRouter.ExecuteSQLRequest(db, sql, row, status);
        if (status.getCode() != 0) {
            logger.error("getInsertRow fail: {}", status.getMsg());
        }
        status.delete();
        return rs;
    }

    @Override
    public Schema getInputSchema(String dbName, String sql) throws SQLException {
        Status status = new Status();
        ExplainInfo explain = sqlRouter.Explain(dbName, sql, status);
        if (status.getCode() != 0 || explain == null) {
            String msg = status.getMsg();
            status.delete();
            if (explain != null) {
                explain.delete();
            }
            throw new SQLException("getInputSchema fail! msg: " + msg);
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
            String msg = status.getMsg();
            status.delete();
            if (procedureInfo != null) {
                procedureInfo.delete();
            }
            throw new SQLException("show procedure failed, msg: " + msg);
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

    static private TableColumnDescPairVector convertSchema(Map<String, Schema> tables) throws SQLException {
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

    static public List<String> genDDL(String sql, Map<String, Map<String, Schema>> tableSchema)
            throws SQLException {
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

    static public Schema genOutputSchema(String sql, Map<String, Map<String, Schema>> tableSchema) throws SQLException {
        if (null == tableSchema || tableSchema.isEmpty()) {
            throw new SQLException("input schema is null or empty");
        }
        TableColumnDescPairVector tableColumnDescPairVector = new TableColumnDescPairVector();
        // TODO(hw): multi db is not supported now, so we add all db-tables here
        for (Map.Entry<String, Map<String, Schema>> entry : tableSchema.entrySet()) {
            Map<String, Schema> schemaMap = entry.getValue();
            tableColumnDescPairVector.addAll(convertSchema(schemaMap));
        }
        com._4paradigm.openmldb.Schema outputSchema = sql_router_sdk.GenOutputSchema(sql, tableColumnDescPairVector);
        // TODO(hw): if we convert com._4paradigm.openmldb.Schema(cPtr) failed, it will throw an exception,
        //  we can't do the later delete()
        Schema ret = Common.convertSchema(outputSchema);
        outputSchema.delete();
        tableColumnDescPairVector.delete();
        return ret;
    }

    @Override
    public boolean createDB(String db) {
        Status status = new Status();
        boolean ok = sqlRouter.CreateDB(db, status);
        if (status.getCode() != 0) {
            logger.error("create db fail: {}", status.getMsg());
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
            logger.error("drop db fail: {}", status.getMsg());
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
            logger.error("showDatabases fail: {}", status.getMsg());
        } else {
            databases.addAll(dbs);
        }

        status.delete();
        dbs.delete();
        return databases;
    }

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
