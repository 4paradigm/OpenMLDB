package com._4paradigm.sql.sdk.impl;

import com._4paradigm.sql.*;
import com._4paradigm.sql.common.LibraryLoader;
import com._4paradigm.sql.jdbc.SQLResultSet;
import com._4paradigm.sql.sdk.*;
import com._4paradigm.sql.sdk.ProcedureInfo;
import com._4paradigm.sql.sdk.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
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
        return ok;
    }

    @Override
    public boolean executeInsert(String db, String sql) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteInsert(db, sql, status);
        if (!ok) {
            logger.error("executeInsert fail: {}", status.getMsg());
        }
        return ok;
    }

    @Override
    public boolean executeInsert(String db, String sql, SQLInsertRow row) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteInsert(db, sql, row, status);
        if (!ok) {
            logger.error("executeInsert fail: {}", status.getMsg());
        }
        return ok;
    }

    @Override
    public boolean executeInsert(String db, String sql, SQLInsertRows rows) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteInsert(db, sql, rows, status);
        if (!ok) {
            logger.error("executeInsert fail: {}", status.getMsg());
        }
        return ok;
    }

    @Override
    public ResultSet executeSQL(String db, String sql) {
        Status status = new Status();
        ResultSet rs = sqlRouter.ExecuteSQL(db, sql, status);
        if (status.getCode() != 0) {
            logger.error("executeSQL fail: {}", status.getMsg());
        }
        return rs;
    }

//    @Override
//    private SQLRequestRow getRequestRow(String db, String sql) {
//        Status status = new Status();
//        SQLRequestRow row = sqlRouter.GetRequestRow(db, sql, status);
//        if (status.getCode() != 0) {
//            logger.error("getRequestRow fail: {}", status.getMsg());
//        }
//        return row;
//    }

    @Override
    public SQLInsertRow getInsertRow(String db, String sql) {
        Status status = new Status();
        SQLInsertRow row = sqlRouter.GetInsertRow(db, sql, status);
        if (status.getCode() != 0) {
            logger.error("getInsertRow fail: {}", status.getMsg());
        }
        return row;
    }

    public PreparedStatement getInsertPreparedStmt(String db, String sql) {
        try {
            InsertPreparedStatementImpl impl = new InsertPreparedStatementImpl(db, sql, this.sqlRouter);
            return impl;
        } catch (Exception e) {
            return null;
        }
    }

    public PreparedStatement getRequestPreparedStmt (String db, String sql) throws SQLException {
        RequestPreparedStatementImpl impl = new RequestPreparedStatementImpl(db, sql, this.sqlRouter);
        return impl;
    }

    public PreparedStatement getBatchRequestPreparedStmt(String db, String sql,
                                                         List<Integer> commonColumnIndices) throws SQLException {
        BatchRequestPreparedStatementImpl impl = new BatchRequestPreparedStatementImpl(
                db, sql, this.sqlRouter, commonColumnIndices);
        return impl;
    }

    public CallablePreparedStatementImpl getCallablePreparedStmt(String db, String spName)  throws SQLException {
        CallablePreparedStatementImpl impl = new CallablePreparedStatementImpl(db, spName, this.sqlRouter);
        return impl;
    }

    @Override
    public BatchCallablePreparedStatementImpl getCallablePreparedStmtBatch(String db, String spName) throws SQLException {
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
        return rows;
    }

    @Override
    public ResultSet executeSQL(String db, String sql, SQLRequestRow row) {
        //TODO(wangtaize) add execption
        Status status = new Status();
        ResultSet rs = sqlRouter.ExecuteSQL(db, sql, row, status);
        if (status.getCode() != 0) {
            logger.error("getInsertRow fail: {}", status.getMsg());
        }
        return rs;
    }

    @Override
    public Schema getInputSchema(String dbName, String sql) throws SQLException {
        Status status = new Status();
        ExplainInfo explain = sqlRouter.Explain(dbName, sql, status);
        if (status.getCode() != 0 || explain == null) {
            throw new SQLException("getInputSchema fail! msg: " + status.getMsg());
        }
        List<Column> columnList = new ArrayList<>();
        com._4paradigm.sql.Schema schema = explain.GetInputSchema();
        for (int i = 0; i < schema.GetColumnCnt(); i++) {
            Column column = new Column();
            column.setColumnName(schema.GetColumnName(i));
            column.setSqlType(Common.type2SqlType(schema.GetColumnType(i)));
            column.setNotNull(schema.IsColumnNotNull(i));
            column.setConstant(false);
            columnList.add(column);
        }
        return new Schema(columnList);
    }

    private com._4paradigm.sql.ProcedureInfo prepareProcedure(String dbName, String spName) throws SQLException {
        if (!sqlRouter.RefreshCatalog()) {
            throw new SQLException("refresh catalog failed!");
        }
        Status status = new Status();
        com._4paradigm.sql.ProcedureInfo procedureInfo = sqlRouter.ShowProcedure(dbName, spName, status);
        if (procedureInfo == null || status.getCode() != 0) {
            throw new SQLException("show procedure failed, msg: " + status.getMsg());
        }
        return procedureInfo;
    }

    private SQLRequestRow getProcedureRequestRow(String dbName,
                                                 com._4paradigm.sql.ProcedureInfo procedureInfo,
                                                 Object[] requestRow) throws SQLException {
        Status status = new Status();
        SQLRequestRow sqlRequestRow = sqlRouter.GetRequestRow(dbName, procedureInfo.GetSql(), status);
        if (status.getCode() != 0 || sqlRequestRow == null) {
            logger.error("getRequestRow failed: {}", status.getMsg());
            throw new SQLException("getRequestRow failed!, msg: " + status.getMsg());
        }
        com._4paradigm.sql.Schema inputSchema = procedureInfo.GetInputSchema();
        if (inputSchema == null) {
            throw new SQLException("inputSchema is null");
        }
        int strlen = 0;
        for (int i = 0 ; i < inputSchema.GetColumnCnt(); i++) {
            DataType dataType = inputSchema.GetColumnType(i);
            Object object = requestRow[i];
            if (dataType != null && dataType == DataType.kTypeString && object != null) {
                try {
                    strlen += ((String)object).getBytes("utf-8").length;
                } catch (UnsupportedEncodingException e) {
                    throw new SQLException(e);
                }
            }
        }
        if (!sqlRequestRow.Init(strlen)) {
            throw new SQLException("init request row failed");
        }
        boolean ok;
        for (int i = 0 ; i < inputSchema.GetColumnCnt(); i++) {
            DataType dataType = inputSchema.GetColumnType(i);
            if (requestRow[i] == null) {
                ok = sqlRequestRow.AppendNULL();
                if (!ok) {
                    throw new SQLException("append data failed, idx is " + i);
                }
                continue;
            }
            switch (Common.type2SqlType(dataType)) {
                case Types.BOOLEAN:
                    ok = sqlRequestRow.AppendBool(Boolean.parseBoolean(requestRow[i].toString()));
                    break;
                case Types.SMALLINT:
                    ok = sqlRequestRow.AppendInt16(Short.parseShort(requestRow[i].toString()));
                    break;
                case Types.INTEGER:
                    ok = sqlRequestRow.AppendInt32(Integer.parseInt(requestRow[i].toString()));
                    break;
                case Types.BIGINT:
                    ok = sqlRequestRow.AppendInt64(Long.parseLong(requestRow[i].toString()));
                    break;
                case Types.FLOAT:
                    ok = sqlRequestRow.AppendFloat(Float.parseFloat(requestRow[i].toString()));
                    break;
                case Types.DOUBLE:
                    ok = sqlRequestRow.AppendDouble(Double.parseDouble(requestRow[i].toString()));
                    break;
                case Types.DATE:
                    java.sql.Date date;
                    if (requestRow[i] instanceof java.sql.Date) {
                        date = (java.sql.Date) requestRow[i];
                    } else {
                        date = java.sql.Date.valueOf(requestRow[i].toString());
                    }
                    ok = sqlRequestRow.AppendDate(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
                    break;
                case Types.TIMESTAMP:
                    if (requestRow[i] instanceof Timestamp) {
                        ok = sqlRequestRow.AppendTimestamp(((Timestamp) requestRow[i]).getTime());
                    } else {
                        ok = sqlRequestRow.AppendTimestamp(Long.parseLong(requestRow[i].toString()));
                    }
                    break;
                case Types.VARCHAR:
                    ok = sqlRequestRow.AppendString((String)requestRow[i]);
                    break;
                default:
                    throw new SQLException("column type not supported");
            }
            if (!ok) {
                throw new SQLException("append data failed, idx is " + i);
            }
        }
        if (!sqlRequestRow.Build()) {
            throw new SQLException("build request row failed");
        }
        return sqlRequestRow;
    }

    private SQLResultSet callProcedureWithSingleRow(String dbName, String spName,
                                                    com._4paradigm.sql.ProcedureInfo procedureInfo,
                                                    Object[] requestRow) throws SQLException {
        if (requestRow == null || requestRow.length == 0) {
            throw new SQLException("requestRow is null or empty");
        }
        SQLRequestRow sqlRequestRow = getProcedureRequestRow(dbName, procedureInfo, requestRow);
        Status status = new Status();
        ResultSet resultSet = sqlRouter.CallProcedure(dbName, spName, sqlRequestRow, status);
        if (resultSet == null || status.getCode() != 0) {
            throw new SQLException("call procedure fail! msg: " + status.getMsg());
        }
        return new SQLResultSet(resultSet);
    }

    @Override
    public ProcedureInfo showProcedure(String dbName, String proName) throws SQLException{
        Status status = new Status();
        com._4paradigm.sql.ProcedureInfo procedureInfo = sqlRouter.ShowProcedure(dbName, proName, status);
        if (procedureInfo == null || status.getCode() != 0) {
            throw new SQLException("show procedure failed, msg: " + status.getMsg());
        }
        ProcedureInfo spInfo = new ProcedureInfo();
        spInfo.setDbName(procedureInfo.GetDbName());
        spInfo.setProName(procedureInfo.GetSpName());
        spInfo.setSql(procedureInfo.GetSql());
        spInfo.setInputSchema(Common.convertSchema(procedureInfo.GetInputSchema()));
        spInfo.setOutputSchema(Common.convertSchema(procedureInfo.GetOutputSchema()));
        spInfo.setMainTable(procedureInfo.GetMainTable());
        spInfo.setInputTables(procedureInfo.GetTables());
        return spInfo;
    }

    @Override
    public boolean createDB(String db) {
        Status status = new Status();
        boolean ok = sqlRouter.CreateDB(db, status);
        if (status.getCode() != 0) {
            logger.error("getInsertRow fail: {}", status.getMsg());
        }
        return ok;
    }

    @Override
    public boolean dropDB(String db) {
        Status status = new Status();
        boolean ok =  sqlRouter.DropDB(db, status);
        if (status.getCode() != 0) {
            logger.error("getInsertRow fail: {}", status.getMsg());
        }
        return ok;
    }

}
