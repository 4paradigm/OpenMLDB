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

import com._4paradigm.openmldb.*;
import com._4paradigm.openmldb.common.LibraryLoader;
import com._4paradigm.openmldb.sdk.*;
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.sdk.*;
import com._4paradigm.openmldb.sdk.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class SqlClusterExecutor implements SqlExecutor {
    static {
        String libname = "sql_jsdk";
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.equals("mac os x")) {
            LibraryLoader.loadLibrary(libname);
        } else {
            LibraryLoader.loadLibrary(libname);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(SqlClusterExecutor.class);
    private SdkOption option;
    private SQLRouter sqlRouter;

    public SqlClusterExecutor(SdkOption option) throws SqlException {
        this.option = option;
        SQLRouterOptions sqlOpt = new SQLRouterOptions();
        sqlOpt.setSession_timeout(option.getSessionTimeout());
        sqlOpt.setZk_cluster(option.getZkCluster());
        sqlOpt.setZk_path(option.getZkPath());
        sqlOpt.setEnable_debug(option.getEnableDebug());
        sqlOpt.setRequest_timeout(option.getRequestTimeout());
        this.sqlRouter = sql_router_sdk.NewClusterSQLRouter(sqlOpt);
        sqlOpt.delete();
        if (sqlRouter == null) {
            SqlException e = new SqlException("fail to create sql executer");
            throw e;
        }
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
        status = null;
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
        status = null;
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
        status = null;
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
        status = null;
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
        status = null;
        return  new SQLResultSet(rs);
    }

    @Override
    public SQLInsertRow getInsertRow(String db, String sql) {
        Status status = new Status();
        SQLInsertRow row = sqlRouter.GetInsertRow(db, sql, status);
        if (status.getCode() != 0) {
            logger.error("getInsertRow fail: {}", status.getMsg());
        }
        status.delete();
        status = null;
        return row;
    }

    public PreparedStatement getInsertPreparedStmt(String db, String sql) throws SQLException {
        InsertPreparedStatementImpl impl = new InsertPreparedStatementImpl(db, sql, this.sqlRouter);
        return impl;
    }

    public PreparedStatement getRequestPreparedStmt(String db, String sql) throws SQLException {
        RequestPreparedStatementImpl impl = new RequestPreparedStatementImpl(db, sql, this.sqlRouter);
        return impl;
    }
    public PreparedStatement getPreparedStatement(String db, String sql) throws SQLException {
        PreparedStatementImpl impl = new PreparedStatementImpl(db, sql, this.sqlRouter);
        return impl;
    }

    public PreparedStatement getBatchRequestPreparedStmt(String db, String sql,
                                                         List<Integer> commonColumnIndices) throws SQLException {
        BatchRequestPreparedStatementImpl impl = new BatchRequestPreparedStatementImpl(
                db, sql, this.sqlRouter, commonColumnIndices);
        return impl;
    }

    public CallablePreparedStatement getCallablePreparedStmt(String db, String spName) throws SQLException {
        CallablePreparedStatementImpl impl = new CallablePreparedStatementImpl(db, spName, this.sqlRouter);
        return impl;
    }

    @Override
    public CallablePreparedStatement getCallablePreparedStmtBatch(String db, String spName) throws SQLException {
        BatchCallablePreparedStatementImpl impl = new BatchCallablePreparedStatementImpl(db, spName, this.sqlRouter);
        return impl;
    }

    @Override
    public SQLInsertRows getInsertRows(String db, String sql) {
        Status status = new Status();
        SQLInsertRows rows = sqlRouter.GetInsertRows(db, sql, status);
        if (status.getCode() != 0) {
            logger.error("getInsertRow fail: {}", status.getMsg());
        }
        status.delete();
        status = null;
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
        status = null;
        return rs;
    }

    @Override
    public Schema getInputSchema(String dbName, String sql) throws SQLException {
        Status status = new Status();
        ExplainInfo explain = sqlRouter.Explain(dbName, sql, status);
        if (status.getCode() != 0 || explain == null) {
            String msg = status.getMsg();
            status.delete();
            status = null;
            if (explain != null) {
                explain.delete();
            }
            throw new SQLException("getInputSchema fail! msg: " + msg);
        }
        status.delete();
        status = null;
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
    public com._4paradigm.openmldb.sdk.ProcedureInfo showProcedure(String dbName, String proName) throws SQLException {
        Status status = new Status();
        com._4paradigm.openmldb.ProcedureInfo procedureInfo = sqlRouter.ShowProcedure(dbName, proName, status);
        if (procedureInfo == null || status.getCode() != 0) {
            String msg = status.getMsg();
            status.delete();
            status = null;
            if (procedureInfo != null) {
                procedureInfo.delete();
            }
            throw new SQLException("show procedure failed, msg: " + msg);
        }
        status.delete();
        status = null;
        com._4paradigm.openmldb.sdk.ProcedureInfo spInfo = new com._4paradigm.openmldb.sdk.ProcedureInfo();
        spInfo.setDbName(procedureInfo.GetDbName());
        spInfo.setProName(procedureInfo.GetSpName());
        spInfo.setSql(procedureInfo.GetSql());
        spInfo.setInputSchema(Common.convertSchema(procedureInfo.GetInputSchema()));
        spInfo.setOutputSchema(Common.convertSchema(procedureInfo.GetOutputSchema()));
        spInfo.setMainTable(procedureInfo.GetMainTable());
        spInfo.setInputTables(procedureInfo.GetTables());
        procedureInfo.delete();
        return spInfo;
    }

    @Override
    public boolean createDB(String db) {
        Status status = new Status();
        boolean ok = sqlRouter.CreateDB(db, status);
        if (status.getCode() != 0) {
            logger.error("create db fail: {}", status.getMsg());
        }
        status.delete();
        status = null;
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
        status = null;
        return ok;
    }

    @Override
    public void close() {
        if (sqlRouter != null) {
            sqlRouter.delete();
            sqlRouter = null;
        }
    }

}
