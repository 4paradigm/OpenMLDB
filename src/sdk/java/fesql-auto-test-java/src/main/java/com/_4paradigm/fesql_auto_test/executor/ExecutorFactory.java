package com._4paradigm.fesql_auto_test.executor;


import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.sql.sdk.SqlExecutor;

import java.util.List;

public class ExecutorFactory {

    public static BaseExecutor build(SqlExecutor executor, SQLCase fesqlCase) {
        return build(executor, fesqlCase, false);
    }

    public static BaseExecutor build(SqlExecutor executor, SQLCase fesqlCase,
                                     boolean requestMode) {
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
        executor = new RequestQuerySQLExecutor(sqlExecutor, fesqlCase, false, false);
        return executor;
    }

    public static BaseExecutor getSQLBatchRequestQueryExecutor(SqlExecutor sqlExecutor,
                                                               SQLCase fesqlCase) {
        RequestQuerySQLExecutor executor = new RequestQuerySQLExecutor(
                sqlExecutor, fesqlCase, true, false);
        return executor;
    }

    public static BaseExecutor getFeRequestQueryWithSpExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, boolean isAsyn) {
        BaseExecutor executor = null;
        executor = new StoredProcedureSQLExecutor(
                sqlExecutor, fesqlCase, fesqlCase.getBatch_request() != null, isAsyn);
        return executor;
    }

}
