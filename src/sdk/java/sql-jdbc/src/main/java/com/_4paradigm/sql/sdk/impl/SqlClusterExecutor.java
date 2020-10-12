package com._4paradigm.sql.sdk.impl;

import com._4paradigm.sql.*;
import com._4paradigm.sql.common.LibraryLoader;
import com._4paradigm.sql.jdbc.SQLResultSet;
import com._4paradigm.sql.sdk.*;
import com._4paradigm.sql.sdk.ProcedureInfo;
import com._4paradigm.sql.sdk.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            throw new SQLException("getInputSchema fail!", status.getMsg());
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


    @Override
    public SQLResultSet callProcedure(String dbName, String proName, Object[] requestRow) throws SQLException {
        Object[][] requestRows = new Object[1][requestRow.length];
        return callProcedure(dbName, proName, requestRows);
    }

    @Override
    public SQLResultSet callProcedure(String dbName, String proName, Object[][] requestRows) throws SQLException {
        if (requestRows == null || requestRows.length == 0) {
            throw new SQLException("requestRows is null or empty");
        }
        Object[] requestRow = requestRows[0];
        if (requestRow == null || requestRow.length == 0) {
            throw new SQLException("requestRow is null or empty");
        }
        ProcedureInfo procedureInfo = showProcedure(dbName, proName);
        Status status = new Status();
        SQLRequestRow sqlRequestRow = sqlRouter.GetRequestRow(dbName, procedureInfo.getSql(), status);
        if (status.getCode() != 0 || sqlRequestRow == null) {
            logger.error("getRequestRow failed: {}", status.getMsg());
            throw new SQLException("getRequestRow failed");
        }
        Schema inputSchema = procedureInfo.getInputSchema();
        int strlen = 0;
        for (int i = 0 ; i < inputSchema.getColumnList().size(); i++) {
            Column column = inputSchema.getColumnList().get(i);
            Object object = requestRow[i];
            if (column.getSqlType() == Types.VARCHAR && object != null) {
                strlen += ((String)object).length();
            }
        }
        if (!sqlRequestRow.Init(strlen)) {
            throw new SQLException("init request row failed");
        }
        for (int i = 0 ; i < inputSchema.getColumnList().size(); i++) {
            Column column = inputSchema.getColumnList().get(i);
            switch (column.getSqlType()) {
                case Types.BOOLEAN:
                    sqlRequestRow.AppendBool((Boolean)requestRow[i]);
                    break;
                case Types.SMALLINT:
                    sqlRequestRow.AppendInt16((Short)requestRow[i]);
                    break;
                case Types.INTEGER:
                    sqlRequestRow.AppendInt32((Integer) requestRow[i]);
                    break;
                case Types.BIGINT:
                    sqlRequestRow.AppendInt64((Long) requestRow[i]);
                    break;
                case Types.FLOAT:
                    sqlRequestRow.AppendFloat((Float) requestRow[i]);
                    break;
                case Types.DOUBLE:
                    sqlRequestRow.AppendDouble((Double) requestRow[i]);
                    break;
                case Types.DATE:
                    java.sql.Date date = (java.sql.Date)requestRow[i];
                    sqlRequestRow.AppendDate(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
                    break;
                case Types.TIMESTAMP:
                    sqlRequestRow.AppendTimestamp(((Timestamp)requestRow[i]).getTime());
                    break;
                case Types.VARCHAR:
                    sqlRequestRow.AppendString((String)requestRow[i]);
                    break;
                default:
                    throw new SQLException("column type not supported");
            }
        }
        sqlRequestRow.Build();
        ResultSet resultSet = sqlRouter.CallProcedure(dbName, proName, sqlRequestRow, status);
        if (resultSet == null || status.getCode() != 0) {
            throw new SQLException("call procedure fail");
        }
        return new SQLResultSet(resultSet);
    }

    @Override
    public boolean dropProcedure(String dbName, String proName)  throws SQLException{
        return false;
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
