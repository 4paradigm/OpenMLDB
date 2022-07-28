package com._4paradigm.openmldb.java_sdk_test.cluster.v060;

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
    @Yaml(filePaths = "function/long_window/test_count_where.yaml")
    @Story("longWindowDeploy")
    public void testLongWindow2(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kLongWindow).run();
    }
}
