package com._4paradigm.fesql_auto_test.executor;


import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.sql.sdk.SqlExecutor;

public class ExecutorFactory {

    public static BaseExecutor build(SqlExecutor executor, SQLCase fesqlCase) {
        if (null == fesqlCase) {
            return new NullExecutor(executor, fesqlCase);
        }
        return getFeExecutor(executor, fesqlCase);
    }

    private static BaseExecutor getFeExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase) {
        BaseExecutor executor = null;
        executor = new SQLExecutor(sqlExecutor, fesqlCase);
        return executor;
    }
}
