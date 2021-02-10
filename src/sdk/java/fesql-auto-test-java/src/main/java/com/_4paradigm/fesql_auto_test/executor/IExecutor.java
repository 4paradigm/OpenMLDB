package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.sql.sdk.SqlExecutor;

/**
 * @author zhaowei
 * @date 2021/2/5 3:31 PM
 */
public interface IExecutor {

    boolean verify();

    void run();

    // void process();

    void prepare() throws Exception;

    FesqlResult execute(String version,SqlExecutor executor);

    // void check(FesqlResult fesqlResult) throws Exception;

    void tearDown();
}
