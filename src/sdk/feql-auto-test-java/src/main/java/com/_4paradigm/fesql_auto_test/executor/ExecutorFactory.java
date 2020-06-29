package com._4paradigm.fesql_auto_test.executor;


import com._4paradigm.fesql_auto_test.entity.FesqlCase;
import com._4paradigm.sql.sdk.SqlExecutor;
import org.testng.Assert;

public class ExecutorFactory {

    public static BaseExecutor build(SqlExecutor executor,FesqlCase fesqlCase) {
        if (null == fesqlCase) {
            return new NullExecutor(executor,fesqlCase);
        }
        return getFeExecutor(executor,fesqlCase);
    }

    private static BaseExecutor getFeExecutor(SqlExecutor sqlExecutor,FesqlCase fesqlCase) {
        BaseExecutor executor = null;
        String executorType = fesqlCase.getExecutor();
        if (null == executorType) {
            executor = new SQLExecutor(sqlExecutor,fesqlCase);
        } else {
            switch (executorType) {
                case "sql":
                    executor = new SQLExecutor(sqlExecutor,fesqlCase);
                    break;
                default:
                    Assert.assertFalse(true,"No executor named: " + executorType);
                    break;
            }
        }
        return executor;
    }
}
