package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;


/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class CountChecker extends BaseChecker {

    public CountChecker(SQLCase fesqlCase, FesqlResult fesqlResult){
        super(fesqlCase,fesqlResult);
    }

    @Override
    public void check() throws Exception {
        log.info("count check");
        int expect =  (int)fesqlCase.getExpect().getCount();
        int actual = fesqlResult.getCount();
        Assert.assertEquals(actual,expect,"count验证失败");
    }

}
