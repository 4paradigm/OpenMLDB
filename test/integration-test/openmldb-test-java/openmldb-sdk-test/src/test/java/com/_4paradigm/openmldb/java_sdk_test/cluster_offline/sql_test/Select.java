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

 package com._4paradigm.openmldb.java_sdk_test.cluster_offline.sql_test;

 import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBTest;
 import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
 import com._4paradigm.openmldb.test_common.model.SQLCase;
 import com._4paradigm.openmldb.test_common.model.SQLCaseType;
 import com._4paradigm.openmldb.test_common.provider.Yaml;
 import io.qameta.allure.Feature;
 import io.qameta.allure.Story;
 import io.qameta.allure.Step;
 import lombok.extern.slf4j.Slf4j;
 import org.testng.annotations.Test;
 

 @Slf4j
 @Feature("OfflineClusterSelect")
 public class Select extends OpenMLDBTest {

    // @Story("Express")
    // @Test(dataProvider = "getCase")
    // @Yaml(filePaths = {
    //         // "integration_test/expression/"
    //         "integration_test/tmp"
    // })
    // public void testExpress(SQLCase testCase) throws Exception {
    //     ExecutorFactory.build(executor, testCase, SQLCaseType.KOfflineJob).run();
    // }

     @Story("Function")
     @Test(dataProvider = "getCase",enabled = true)
     @Yaml(filePaths = "integration_test/function_tobedev/")
     public void testFunctionTobedev(SQLCase testCase) throws Exception {
         ExecutorFactory.build(executor, testCase, SQLCaseType.KOfflineJob).run();
     }

    @Story("Function")
    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = "integration_test/function/")
    public void testFunction(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, SQLCaseType.KOfflineJob).run();
    }

    @Story("Last_join")
    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = {"integration_test/join/"})
    public void testLastJoin(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, SQLCaseType.KOfflineJob).run();
    }

    @Story("Multiple_databases")
    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = {"integration_test/multiple_databases/"})
    @Step("{testCase.desc}")
    public void testMultiDB(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, SQLCaseType.KOfflineJob).run();
    }

    // @Story("Const_query")
    // @Test(dataProvider = "getCase")
    // @Yaml(filePaths = {"integration_test/select/","query/const_query.yaml"})
    // @Step("{testCase.desc}")
    // public void testSelect(SQLCase testCase) throws Exception {
    //     ExecutorFactory.build(executor, testCase, SQLCaseType.KOfflineJob).run();
    // }

    // @Story("Window")
    // @Test(dataProvider = "getCase")
    // @Yaml(filePaths = {"integration_test/window/",
    //         "integration_test/cluster/",
    //         "integration_test/test_index_optimized.yaml"})
    // public void testWindowBatch(SQLCase testCase) throws Exception {
    //     ExecutorFactory.build(executor, testCase, SQLCaseType.KOfflineJob).run();
    // }
 }
 