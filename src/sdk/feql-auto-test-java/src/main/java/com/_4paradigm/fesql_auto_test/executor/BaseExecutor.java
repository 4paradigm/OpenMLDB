package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql_auto_test.checker.Checker;
import com._4paradigm.fesql_auto_test.checker.CheckerStrategy;
import com._4paradigm.fesql_auto_test.entity.FesqlCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/15 11:23 AM
 */
@Slf4j
public abstract class BaseExecutor {
    protected FesqlCase fesqlCase;
    protected SqlExecutor executor;

    public BaseExecutor(SqlExecutor executor,FesqlCase fesqlCase) {
        this.executor = executor;
        this.fesqlCase = fesqlCase;
    }

    public void run() throws Exception {
        log.info(fesqlCase.getDesc() + " Begin!");
        if (null == fesqlCase) {
            return;
        }
        prepare();
        FesqlResult fesqlResult = execute();
        if(fesqlCase.getCheck_sql()!=null) {
            fesqlResult = after();
        }
        check(fesqlResult);
        tearDown();

    }
    protected abstract void prepare() throws Exception;
    protected abstract FesqlResult execute() throws Exception;
    protected FesqlResult after(){
        return null;
    }
    protected void check(FesqlResult fesqlResult) throws Exception {
        List<Checker> strategyList = CheckerStrategy.build(fesqlCase, fesqlResult);
        for (Checker checker : strategyList) {
            checker.check();
        }
    }
    protected void tearDown(){}
}
