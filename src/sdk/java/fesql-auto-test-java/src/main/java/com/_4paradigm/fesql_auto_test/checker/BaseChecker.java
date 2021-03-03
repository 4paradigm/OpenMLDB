package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.ExpectDesc;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.ReportLog;

import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/16 3:37 PM
 */
public abstract class BaseChecker implements Checker {
    protected FesqlResult fesqlResult;
    protected Map<String,FesqlResult> resultMap;
    protected ExpectDesc expect;
    protected ReportLog reportLog = ReportLog.of();

    public BaseChecker(ExpectDesc expect, FesqlResult fesqlResult){
        this.expect = expect;
        this.fesqlResult = fesqlResult;
    }

    public BaseChecker(FesqlResult fesqlResult,Map<String,FesqlResult> resultMap){
        this.fesqlResult = fesqlResult;
        this.resultMap = resultMap;
    }
}
