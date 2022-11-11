package com._4paradigm.openmldb.java_sdk_test.cluster.out_in;

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
@Feature("Task")
public class OutInTest extends OpenMLDBTest {
    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = "integration_test/out_in/test_job.yaml")
    @Story("Job")
    public void testJob(SQLCase testCase){
        System.out.println("testCase = " + testCase);
        ExecutorFactory.build(executor, testCase, SQLCaseType.kBatch).run();
    }

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/out_in/test_select_into_load_data.yaml")
    @Story("Out-In")
    public void testOutIn(SQLCase testCase){
        System.out.println("testCase = " + testCase);
        ExecutorFactory.build(executor, testCase, SQLCaseType.kJob).run();
    }



}