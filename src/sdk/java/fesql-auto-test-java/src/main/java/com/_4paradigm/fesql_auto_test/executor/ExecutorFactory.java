package com._4paradigm.fesql_auto_test.executor;


import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.sql.sdk.SqlExecutor;

public class ExecutorFactory {

    public static BaseExecutor build(SqlExecutor executor, SQLCase fesqlCase) {
        return build(executor, fesqlCase, false);
    }
    public static BaseExecutor build(SqlExecutor executor, SQLCase fesqlCase, boolean requestMode) {
        if (null == fesqlCase) {
            return new NullExecutor(executor, fesqlCase);
        }

        if (requestMode) {
            return getFeRequestQueryExecutor(executor,fesqlCase);
        } else {
            return getFeExecutor(executor, fesqlCase);
        }
    }

    private static BaseExecutor getFeExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase) {
        BaseExecutor executor = null;
        executor = new SQLExecutor(sqlExecutor, fesqlCase);
        return executor;
    }

    private static BaseExecutor getFeRequestQueryExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase) {
        BaseExecutor executor = null;
        executor = new RequestQuerySQLExecutor(sqlExecutor, fesqlCase);
        return executor;
    }

    public static BaseExecutor getFeRequestQueryWithSpExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase) {
        BaseExecutor executor = null;
        executor = new StoredProcedureSQLExecutor(sqlExecutor, fesqlCase);
        return executor;
    }

}
