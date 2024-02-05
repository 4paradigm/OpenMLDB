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
package com._4paradigm.openmldb.http_test.tmp;

import com._4paradigm.openmldb.http_test.executor.RestfulOnlineExecutor;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.testng.annotations.Test;
import com._4paradigm.openmldb.test_common.common.BaseTest;
import com._4paradigm.openmldb.test_common.model.SQLCase;

@Feature("MultiDB api request")
public class TestMultiDB  {
    @Test(dataProvider = "getCase",dataProviderClass = BaseTest.class)
    @Yaml(filePaths = "integration_test/multiple_databases/")
    @Story("MultiDB api request")
     public void testBatch(SQLCase sqlCase){
        new RestfulOnlineExecutor(sqlCase).run();
    }


}