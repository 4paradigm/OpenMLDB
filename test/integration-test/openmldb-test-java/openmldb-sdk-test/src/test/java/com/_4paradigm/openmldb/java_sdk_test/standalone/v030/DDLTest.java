/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
@Slf4j
@Feature("CLI-DDL")
public class DDLTest extends StandaloneTest {
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/ddl/test_create.yaml")
    @Story("create")
    public void testCreate(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kStandaloneCLI).run();
    }

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/ddl/test_ttl.yaml")
    @Story("ttl")
    public void testTTL(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kStandaloneCLI).run();
    }

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/ddl/test_options.yaml")
    @Story("options")
    public void testOptions(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kStandaloneCLI).run();
    }

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/ddl/test_create_index.yaml")
    @Story("create_index")
    public void testCreateIndex(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kStandaloneCLI).run();
    }

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/ddl/test_create_no_index.yaml")
    @Story("create_no_index")
    public void testCreateNoIndex(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kStandaloneCLI).run();
    }

    //SDK版本

    //all-pass
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/ddl/test_create.yaml")
    @Story("create")
    public void testCreateSDk(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }

    //全pass
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/ddl/test_ttl.yaml")
    @Story("ttl")
    public void testTTLSDK(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }

    //pass 单机
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/ddl/test_options.yaml")
    @Story("options")
    public void testOptionsSDK(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }

    //all pass
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/ddl/test_create_index.yaml")
    @Story("create_index")
    public void testCreateIndexSDK(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }

    //all pass
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/ddl/test_create_no_index.yaml")
    @Story("create_no_index")
    public void testCreateNoIndexSDK(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/ddl/test_delete_index.yaml")
    @Story("create_index")
    public void testDeleteIndex(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kDDL).run();
    }
    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = "integration_test/ddl/test_delete_index.yaml")
    @Story("create_index")
    public void testDeleteIndexByCli(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kClusterCLI).run();
    }

}
