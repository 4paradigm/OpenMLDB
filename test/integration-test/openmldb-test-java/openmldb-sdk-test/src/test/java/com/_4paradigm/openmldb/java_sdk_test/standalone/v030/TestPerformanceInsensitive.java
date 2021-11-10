package com._4paradigm.openmldb.java_sdk_test.standalone.v030;

import com._4paradigm.openmldb.java_sdk_test.common.StandaloneTest;
import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;
@Slf4j
@Feature("PerformanceInsensitive")
public class TestPerformanceInsensitive extends StandaloneTest {

    // @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/test_performance_insensitive/test_performance_insensitive.yaml")
    @Story("batch")
    public void testPerformanceInsensitive(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kStandaloneCLI).run();
    }
}
