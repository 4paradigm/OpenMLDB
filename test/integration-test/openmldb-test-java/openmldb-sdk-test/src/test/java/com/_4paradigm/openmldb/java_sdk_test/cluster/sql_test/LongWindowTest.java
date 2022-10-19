package com._4paradigm.openmldb.java_sdk_test.cluster.sql_test;

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBTest;
import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
@Feature("long_window")
public class LongWindowTest extends OpenMLDBTest {

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/long_window/test_long_window.yaml")
    @Story("longWindow")
    public void testLongWindow(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kLongWindow).run();
    }
    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = "integration_test/long_window/test_long_window_batch.yaml")
    @Story("longWindow-batch")
    public void testLongWindowByBatch(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/long_window/test_count_where.yaml")
    @Story("count_where")
    public void testCountWhere(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kLongWindow).run();
    }

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/long_window/test_xxx_where.yaml")
    @Story("xxx_where")
    public void testXXXWhere(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kLongWindow).run();
    }
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/long_window/test_udaf.yaml")
    @Story("udaf")
    public void testUDAF(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kLongWindow).run();
    }

}
