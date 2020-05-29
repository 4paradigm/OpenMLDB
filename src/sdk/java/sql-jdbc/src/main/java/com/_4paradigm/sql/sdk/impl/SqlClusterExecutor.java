package com._4paradigm.sql.sdk.impl;

import com._4paradigm.sql.*;
import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.SqlException;
import com._4paradigm.sql.sdk.SqlExecutor;

public class SqlClusterExecutor implements SqlExecutor {
    static {
        String os = System.getProperty("os.name").toLowerCase();
        String path = SqlClusterExecutor.class.getResource("/libsql_jsdk.so").getPath();
        System.load(path);
    }
    private SdkOption option;
    private SQLRouter sqlRouter;
    public SqlClusterExecutor(SdkOption option) throws SqlException{
        this.option = option;
        SQLRouterOptions sqlOpt = new SQLRouterOptions();
        sqlOpt.setSession_timeout(option.getSessionTimeout());
        sqlOpt.setZk_cluster(option.getZkCluster());
        sqlOpt.setZk_path(option.getZkPath());
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
        }
        return ok;
    }

    @Override
    public boolean executeInsert(String db, String sql) {
        Status status = new Status();
        boolean ok = sqlRouter.ExecuteInsert(db, sql, status);
        return ok;
    }

    @Override
    public ResultSet executeSQL(String db, String sql) {
        Status status = new Status();
        ResultSet rs = sqlRouter.ExecuteSQL(db, sql, status);
        return rs;
    }

    @Override
    public SQLRequestRow getRequestRow(String db, String sql) {
        Status status = new Status();
        return sqlRouter.GetRequestRow(db, sql, status);
    }

    @Override
    public ResultSet executeSQL(String db, String sql, SQLRequestRow row) {
        //TODO(wangtaize) add execption
        Status status = new Status();
        return sqlRouter.ExecuteSQL(db, sql, row, status);
    }

    @Override
    public boolean createDB(String db) {
        Status status = new Status();
        return sqlRouter.CreateDB(db, status);
    }
}
