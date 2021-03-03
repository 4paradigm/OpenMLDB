package com._4paradigm.fesql_auto_test.v1;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql.sqlcase.model.SQLCaseType;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProviderList;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;

/**
 * @author chenjing
 * @date 2021/2/2
 */
@Slf4j
@Feature("FZCase")
public class FZCaseTest extends FesqlTest {

    @DataProvider
    public Object[] testFZCaseData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{
                        "/integration/fz_ddl/",
                });
        return dp.getCases().toArray();
    }
    @Story("batch")
    @Test(dataProvider = "testFZCaseData", enabled = false)
    public void testFZCaseBatch(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kBatch).run();
    }
    @Story("request")
    @Test(dataProvider = "testFZCaseData", enabled = false)
    public void testFZCaseRequestMode(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kRequest).run();
    }
    @Story("requestWithSp")
    @Test(dataProvider = "testFZCaseData", enabled = false)
    public void testFZCaseRequestModeWithSp(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kRequestWithSp).run();
    }
    @Story("requestWithSpAysn")
    @Test(dataProvider = "testFZCaseData", enabled = false)
    public void testFZCaseRequestModeWithSpAsync(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kRequestWithSpAsync).run();
    }
}
