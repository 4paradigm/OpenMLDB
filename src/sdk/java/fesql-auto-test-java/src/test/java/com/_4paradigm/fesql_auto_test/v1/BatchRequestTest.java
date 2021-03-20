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

package com._4paradigm.fesql_auto_test.v1;

import com._4paradigm.hybridse.sqlcase.model.SQLCase;
import com._4paradigm.hybridse.sqlcase.model.SQLCaseType;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProviderList;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;

@Feature("BatchTest")
public class BatchRequestTest extends FesqlTest {
    @Story("BatchRequest")
    @Test(dataProvider = "testBatchRequestData")
    public void testBatchRequest(SQLCase testCase) {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kBatchRequest).run();
    }
    @Story("SPBatchRequest")
    @Test(dataProvider = "testBatchRequestData")
    public void testSPBatchRequest(SQLCase testCase) {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kBatchRequestWithSp).run();
    }
    @Story("SPBatchRequestAsyn")
    @Test(dataProvider = "testBatchRequestData")
    public void testSPBatchRequestAsyn(SQLCase testCase) {
        ExecutorFactory.build(executor, testCase, SQLCaseType.kBatchRequestWithSpAsync).run();
    }

    @DataProvider
    public Object[] testBatchRequestData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/integration/v1/test_batch_request.yaml"});
        return dp.getCases().toArray();

    }
}
