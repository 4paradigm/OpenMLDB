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
@Feature("Out-In")
public class OutInTest extends StandaloneTest {
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/out_in/test_out_in.yaml")
    @Story("Out-In")
    public void testOutIn(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kStandaloneCLI).run();
    }

    //11 17 18没有pass
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/out_in/test_out_in.yaml")
    @Story("Out-In")
    public void testOutInSDK(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }
}