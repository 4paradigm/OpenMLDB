package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhaowei
 * @date 2021/2/10 10:13 AM
 */
@Slf4j
public class NullExecutor extends BaseSQLExecutor {

    public NullExecutor(SqlExecutor executor, SQLCase fesqlCase, ExecutorFactory.ExecutorType executorType) {
        super(executor, fesqlCase, executorType);
    }

    @Override
    public FesqlResult execute(String version, SqlExecutor executor) {
        return null;
    }

    @Override
    protected void prepare(String mainVersion, SqlExecutor executor) {

    }

    @Override
    public boolean verify() {
        log.info("No case need to be run.");
        return false;
    }
}
