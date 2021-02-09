package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;

import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/16 3:37 PM
 */
public abstract class BaseChecker implements Checker {
    protected SQLCase fesqlCase;
    protected FesqlResult fesqlResult;
    protected Map<String,FesqlResult> resultMap;
    public BaseChecker(SQLCase fesqlCase, FesqlResult fesqlResult){
        this.fesqlCase = fesqlCase;
        this.fesqlResult = fesqlResult;
    }
    public BaseChecker(SQLCase fesqlCase, FesqlResult fesqlResult,Map<String,FesqlResult> resultMap){
        this(fesqlCase,fesqlResult);
        this.resultMap = resultMap;
    }
}
