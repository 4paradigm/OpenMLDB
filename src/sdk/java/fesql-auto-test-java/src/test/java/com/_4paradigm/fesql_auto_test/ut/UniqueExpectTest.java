package com._4paradigm.fesql_auto_test.ut;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProviderList;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
@Slf4j
@Feature("UT")
public class UniqueExpectTest extends FesqlTest {

    @DataProvider()
    public Object[] getData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/integration/ut_case/test_unique_expect.yaml"});
        return dp.getCases().toArray();
    }

    @Story("batch")
    @Test(dataProvider = "getData")
    @Step("{testCase.desc}")
    public void testUniqueExpect(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kBatch).run();
    }
    @Story("request")
    @Test(dataProvider = "getData")
    public void testUniqueExpectRequestMode(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequest).run();
    }
    @Story("requestWithSp")
    @Test(dataProvider = "getData")
    public void testUniqueExpectRequestModeWithSp(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequestWithSp).run();
    }
    @Story("requestWithSpAysn")
    @Test(dataProvider = "getData")
    public void testUniqueExpectRequestModeWithSpAysn(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequestWithSpAsync).run();
    }

}
