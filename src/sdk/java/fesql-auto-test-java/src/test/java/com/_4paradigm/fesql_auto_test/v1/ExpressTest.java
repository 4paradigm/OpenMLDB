package com._4paradigm.fesql_auto_test.v1;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProviderList;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
@Slf4j
public class ExpressTest extends FesqlTest {

    @DataProvider
    public Object[] testExpressCase() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList.dataProviderGenerator(
                new String[]{
                        "/integration/v1/expression/test_compare.yaml"
                });
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "testExpressCase")
    public void testExpress(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kBatch).run();
    }
    @Test(dataProvider = "testExpressCase")
    public void testExpressRequestMode(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequest).run();
    }
    @Test(dataProvider = "testExpressCase")
    public void testExpressRequestModeWithSp(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequestWithSp).run();
    }
    @Test(dataProvider = "testExpressCase")
    public void testExpressRequestModeWithSpAysn(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequestWithSpAsync).run();
    }
}
