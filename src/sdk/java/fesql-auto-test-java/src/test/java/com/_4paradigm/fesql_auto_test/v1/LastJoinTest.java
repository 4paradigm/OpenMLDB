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
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "testLastJoinData")
    public void testLastJoin(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, ExecutorFactory.ExecutorType.kBatch).run();
    }
    @Test(dataProvider = "testLastJoinData")
    public void testLastJoinRequestMode(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, ExecutorFactory.ExecutorType.kRequest).run();
    }
    @Test(dataProvider = "testLastJoinData")
    public void testLastJoinRequestModeWithSp(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, ExecutorFactory.ExecutorType.kRequestWithSp).run();
    }
    @Test(dataProvider = "testLastJoinData")
    public void testLastJoinRequestModeWithSpAsync(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, ExecutorFactory.ExecutorType.kRequestWithSpAsync).run();
    }

    @DataProvider
    public Object[] testClusterWindowAndLastJoinData() throws FileNotFoundException {
        FesqlDataProvider dp = FesqlDataProvider
                .dataProviderGenerator("/integration/cluster/window_and_lastjoin.yaml");
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "testWindowAndLastJoinData")
    public void testWindowAndLastJoin(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, ExecutorFactory.ExecutorType.kBatch).run();
    }
    @Test(dataProvider = "testClusterWindowAndLastJoinData")
    public void testWindowAndLastJoinRequestMode(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, ExecutorFactory.ExecutorType.kRequest).run();
    }
    @Test(dataProvider = "testClusterWindowAndLastJoinData")
    public void testWindowAndLastJoinDataRequestModeWithSp(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, ExecutorFactory.ExecutorType.kRequestWithSp).run();
    }
    @Test(dataProvider = "testClusterWindowAndLastJoinData")
    public void testWindowAndLastJoinDataRequestModeWithSpAsync(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, ExecutorFactory.ExecutorType.kRequestWithSpAsync).run();
    }

}
