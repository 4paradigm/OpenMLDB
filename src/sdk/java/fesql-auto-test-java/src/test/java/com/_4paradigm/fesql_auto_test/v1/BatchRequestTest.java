package com._4paradigm.fesql_auto_test.v1;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProvider;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.util.List;

public class BatchRequestTest extends FesqlTest {

    @Test(dataProvider = "testBatchRequestData")
    public void testBatchRequest(SQLCase testCase) {
        ExecutorFactory.getSQLBatchRequestQueryExecutor(executor, testCase).run();
    }

    @DataProvider
    public Object[] testBatchRequestData() throws FileNotFoundException {
        FesqlDataProvider dp = FesqlDataProvider
                .dataProviderGenerator("/integration/v1/test_batch_request.yaml");
        return dp.getCases().toArray();
    }
}
