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

package com._4paradigm.openmldb.java_sdk_test.standalone.v040;

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
@Feature("Function-Cli")
public class FunctionTest extends StandaloneTest {

    @Story("like_match")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/v040/test_like_match.yaml")
    public void testLikeMatch(SQLCase testCase) throws Exception {
        ExecutorFactory.build(testCase, SQLCaseType.kStandaloneCLI).run();
    }

    @Story("udaf")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/v040/test_udaf.yaml")
    public void testUDAF(SQLCase testCase) throws Exception {
        ExecutorFactory.build(testCase, SQLCaseType.kStandaloneCLI).run();
    }

    //pass
    @Story("like_match")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/v040/test_like_match.yaml")
    public void testLikeMatchSDK(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }

    //pass
    @Story("udaf")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/v040/test_udaf.yaml")
    public void testUDAFSDK(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }
}
