package com._4paradigm.fesql_auto_test.v1;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProviderList;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;

public class BatchRequestTest extends FesqlTest {

    @Test(dataProvider = "testBatchRequestData")
    public void testBatchRequest(SQLCase testCase) {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kBatchRequest).run();
    }

    @Test(dataProvider = "testBatchRequestData")
    public void testSPBatchRequest(SQLCase testCase) {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kBatchRequestWithSp).run();
    }

    @Test(dataProvider = "testBatchRequestData")
    public void testSPBatchRequestAsyn(SQLCase testCase) {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kBatchRequestWithSpAsync).run();
    }

    @DataProvider
    public Object[] testBatchRequestData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/integration/v1/test_batch_request.yaml"});
        return dp.getCases().toArray();

    }
}
