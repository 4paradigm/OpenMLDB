package com._4paradigm.openmldb.java_sdk_test.v030;

import com._4paradigm.openmldb.java_sdk_test.common.FedbTest;
import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import io.qameta.allure.Feature;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;
@Slf4j
@Feature("PerformanceInsensitive")
public class TestPerformanceInsensitive {

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/out_in/test_performance_insensitive.yaml")
    public void testPerformanceInsensitive(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kCLI).run();
    }
}
