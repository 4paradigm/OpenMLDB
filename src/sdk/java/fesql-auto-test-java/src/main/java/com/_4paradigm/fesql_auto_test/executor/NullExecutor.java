package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NullExecutor extends BaseExecutor {

    public NullExecutor(SqlExecutor executor, SQLCase fesqlCase) {
        super(executor, fesqlCase);
    }

    @Override
    public void prepare() {

    }

    @Override
    public FesqlResult execute() {
        return null;
    }

    public void run() {
        log.info("No case need to be run.");
    }
}
