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

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
@Slf4j
@Feature("DML")
public class DMLTest extends OpenMLDBTest {

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = {"integration_test/dml/test_insert.yaml"})
    @Story("insert")
    public void testInsert(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kDDL).run();
    }

    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = {"integration_test/dml/test_insert.yaml"})
    @Story("insert")
    public void testInsertByCli(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kClusterCLI).run();
    }

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/dml/test_insert_prepared.yaml")
    @Story("insert-prepared")
    public void testInsertWithPrepared(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kInsertPrepared).run();
    }

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/dml/multi_insert.yaml")
    @Story("multi-insert")
    public void testMultiInsert(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kDDL).run();
    }

    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = "integration_test/dml/multi_insert.yaml")
    @Story("multi-insert")
    public void testMultiInsertByCli(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kClusterCLI).run();
    }
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = {"integration_test/dml/test_delete.yaml"})
    @Story("delete")
    public void testDelete(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kDDL).run();
    }

    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = {"integration_test/dml/test_delete.yaml"})
    @Story("delete")
    public void testDeleteByCli(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kClusterCLI).run();
    }

}
