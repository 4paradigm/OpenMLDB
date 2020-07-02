package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql_auto_test.entity.FesqlCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;

/**
 * @author zhaowei
 * @date 2020/6/16 3:37 PM
 */
public abstract class BaseChecker implements Checker {
    protected FesqlCase fesqlCase;
    protected FesqlResult fesqlResult;
    public BaseChecker(FesqlCase fesqlCase,FesqlResult fesqlResult){
        this.fesqlCase = fesqlCase;
        this.fesqlResult = fesqlResult;
    }
}
