package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql_auto_test.checker.Checker;
import com._4paradigm.fesql_auto_test.checker.CheckerStrategy;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import org.testng.Assert;

import java.util.List;

/**
 * @author zhaowei
 * @date 2021/2/5 3:31 PM
 */
public interface IExecutor {

    boolean verify();

    void run();

    // void process();

    void prepare() throws Exception;

    // FesqlResult execute() throws Exception;

    // void check(FesqlResult fesqlResult) throws Exception;

    void tearDown();
}
