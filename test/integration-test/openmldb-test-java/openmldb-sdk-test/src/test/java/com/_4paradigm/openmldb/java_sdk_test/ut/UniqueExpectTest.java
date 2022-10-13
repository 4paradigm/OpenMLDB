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

package com._4paradigm.openmldb.java_sdk_test.ut;

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBTest;
import com._4paradigm.openmldb.test_common.model.OpenMLDBCaseFileList;
import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
@Slf4j
@Feature("UT")
public class UniqueExpectTest extends OpenMLDBTest {

    @DataProvider()
    public Object[] getData() throws FileNotFoundException {
        OpenMLDBCaseFileList dp = OpenMLDBCaseFileList
                .dataProviderGenerator(new String[]{"/integration/ut_case/test_unique_expect.yaml"});
        return dp.getCases().toArray();
    }

    @Story("batch")
    @Test(dataProvider = "getData")
    @Step("{testCase.desc}")
    public void testUniqueExpect(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kBatch).run();
    }
    @Story("request")
    @Test(dataProvider = "getData")
    public void testUniqueExpectRequestMode(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kRequest).run();
    }
    @Story("requestWithSp")
    @Test(dataProvider = "getData")
    public void testUniqueExpectRequestModeWithSp(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kRequestWithSp).run();
    }
    @Story("requestWithSpAysn")
    @Test(dataProvider = "getData")
    public void testUniqueExpectRequestModeWithSpAysn(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kRequestWithSpAsync).run();
    }

}
