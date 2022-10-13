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
import org.testng.annotations.Test;

@Feature("BatchTest")
public class BatchRequestTest extends OpenMLDBTest {
    @Story("BatchRequest")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/test_batch_request.yaml")
    public void testBatchRequest(SQLCase testCase) {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kBatchRequest).run();
    }
    @Story("SPBatchRequest")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/test_batch_request.yaml")
    public void testSPBatchRequest(SQLCase testCase) {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kBatchRequestWithSp).run();
    }
    @Story("SPBatchRequestAsyn")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "integration_test/test_batch_request.yaml")
    public void testSPBatchRequestAsyn(SQLCase testCase) {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kBatchRequestWithSpAsync).run();
    }
}
