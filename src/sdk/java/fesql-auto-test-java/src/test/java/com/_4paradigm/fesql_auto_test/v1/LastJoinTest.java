package com._4paradigm.fesql_auto_test.v1;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProvider;
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
public class LastJoinTest extends FesqlTest {

    @DataProvider
    public Object[] testLastJoinData() throws FileNotFoundException {
        FesqlDataProvider dp = FesqlDataProvider
                .dataProviderGenerator("/integration/v1/test_last_join.yaml");
        return dp.getCases();
    }

    @Test(enabled = false, dataProvider = "testLastJoinData")
    public void testLastJoin(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase).run();
    }
}
