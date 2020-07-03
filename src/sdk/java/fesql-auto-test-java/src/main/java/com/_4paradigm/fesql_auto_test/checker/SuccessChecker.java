package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql_auto_test.entity.FesqlCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class SuccessChecker extends BaseChecker {

    public SuccessChecker(FesqlCase fesqlCase, FesqlResult fesqlResult){
        super(fesqlCase,fesqlResult);
    }

    @Override
    public void check() throws Exception {
        log.info("success check");
        boolean expect =  (boolean)fesqlCase.getExpect().get("success");
        boolean actual = fesqlResult.isOk();
        Assert.assertEquals(actual,expect);
    }
}
