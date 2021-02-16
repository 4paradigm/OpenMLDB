package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.ExpectDesc;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import org.testng.Assert;


/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
// @Slf4j
public class CountChecker extends BaseChecker {

    public CountChecker(ExpectDesc expect, FesqlResult fesqlResult){
        super(expect,fesqlResult);
    }

    @Override
    public void check() throws Exception {
        log.info("count check");
        int expectCount = expect.getCount();
        int actual = fesqlResult.getCount();
        Assert.assertEquals(actual,expectCount,"count验证失败");
    }

}
