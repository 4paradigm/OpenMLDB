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

package com._4paradigm.openmldb.java_sdk_test.cluster.v040;

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
@Feature("deploy")
public class DeploymentTest extends OpenMLDBTest {
    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = "function/deploy/test_create_deploy.yaml")
    @Story("create")
    public void testCreate(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kClusterCLI).run();
    }
    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = "function/deploy/test_show_deploy.yaml")
    @Story("show")
    public void testShow(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kClusterCLI).run();
    }
    @Test(dataProvider = "getCase",enabled = false)
    @Yaml(filePaths = "function/deploy/test_drop_deploy.yaml")
    @Story("drop")
    public void testDrop(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kClusterCLI).run();
    }

}
