package com._4paradigm.openmldb.java_sdk_test.v230;

import com._4paradigm.openmldb.java_sdk_test.common.FedbTest;
import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
@Feature("ParameterQueryTest")
public class ParameterQueryTest extends FedbTest {
    @Story("batch")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = {"query/parameterized_query.yaml"})
    @Step("{testCase.desc}")
    public void testSelect(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kSelectPrepared).run();
    }
}
