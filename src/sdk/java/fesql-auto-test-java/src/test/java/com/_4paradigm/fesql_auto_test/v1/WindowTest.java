package com._4paradigm.fesql_auto_test.v1;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProviderList;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
// @Slf4j
@Feature("Window")
public class WindowTest extends FesqlTest {

    @DataProvider
    public Object[] testWindowData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{
                        "/integration/v1/window/",
                        "/integration/cluster/test_window_row.yaml",
                        "/integration/cluster/test_window_row_range.yaml",
                        "/integration/cluster/window_and_lastjoin.yaml",
                        "/integration/v1/test_index_optimized.yaml",
                });
        return dp.getCases().toArray();
    }
    @Story("batch")
    @Test(dataProvider = "testWindowData")
    public void testWindowBatch(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kBatch).run();
    }
    @Story("request")
    @Test(dataProvider = "testWindowData")
    public void testWindowRequestMode(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequest).run();
    }
    @Story("requestWithSp")
    @Test(dataProvider = "testWindowData")
    public void testWindowRequestModeWithSp(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequestWithSp).run();
    }
    @Story("requestWithSpAysn")
    @Test(dataProvider = "testWindowData")
    public void testWindowRequestModeWithSpAsync(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, ExecutorFactory.ExecutorType.kRequestWithSpAsync).run();
    }
}