package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.ExpectDesc;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class SuccessChecker extends BaseChecker {

    public SuccessChecker(ExpectDesc expect, FesqlResult fesqlResult){
        super(expect,fesqlResult);
    }

    @Override
    public void check() throws Exception {
        log.info("success check");
        boolean success =  expect.getSuccess();
        boolean actual = fesqlResult.isOk();
        Assert.assertEquals(actual,success,"success验证失败");
    }
}
