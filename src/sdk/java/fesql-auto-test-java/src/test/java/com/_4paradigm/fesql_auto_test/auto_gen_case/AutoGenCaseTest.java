package com._4paradigm.fesql_auto_test.auto_gen_case;

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
 * @date 2020/12/28 1:05 PM
 */
@Slf4j
public class AutoGenCaseTest extends FesqlTest {

    @DataProvider()
    public Object[] getCreateData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/auto_gen_cases"});
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getCreateData")
    public void testCaseBatch(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kBatch).run();
    }
    @Test(dataProvider = "getCreateData")
    public void testCaseRequestMode(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequest).run();
    }
    @Test(dataProvider = "getCreateData")
    public void testCaseRequestModeWithSp(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequestWithSp).run();
    }
    @Test(dataProvider = "getCreateData")
    public void testCaseRequestModeWithSpAsync(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequestWithSpAsync).run();
    }
}
