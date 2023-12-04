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


import com._4paradigm.openmldb.http_test.common.ClusterTest;
import com._4paradigm.openmldb.http_test.executor.RestfulOnlineExecutor;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCase;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import org.testng.annotations.Test;
import com._4paradigm.openmldb.test_common.common.BaseTest;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.http_test.common.ClusterTest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Feature("api request")
public class TestTmp  {
    // @Test(dataProvider = "getCase",dataProviderClass = BaseTest.class)
    // @Yaml(filePaths = "integration_test/function/")
    // @Story("api request")
    //  public void testBatch(SQLCase sqlCase){
    //     new RestfulOnlineExecutor(sqlCase).run();
    // }

    // @Test(dataProvider = "getCase",dataProviderClass = BaseTest.class)
    // @Yaml(filePaths = "integration_test/expression/")
    // @Story("api request")
    //  public void testBatch(SQLCase sqlCase){
    //     new RestfulOnlineExecutor(sqlCase).run();
    // }

    // @Test(dataProvider = "getCase",dataProviderClass = BaseTest.class)
    // @Yaml(filePaths = "integration_test/long_window/")
    // @Story("api request")
    //  public void testBatch(SQLCase sqlCase){
    //     new RestfulOnlineExecutor(sqlCase).run();
    // }


    // @Test(dataProvider = "getCase",dataProviderClass = BaseTest.class)
    // @Yaml(filePaths ="integration_test/select/")
    // @Story("api request")
    //  public void testBatch(SQLCase sqlCase){
    //     new RestfulOnlineExecutor(sqlCase).run();
    // }

    // TODO : issued
    // @Test(dataProvider = "getCase",dataProviderClass = BaseTest.class)
    // @Yaml(filePaths ="integration_test/join/")
    // @Story("api request")
    //  public void testBatch(SQLCase sqlCase){
    //     new RestfulOnlineExecutor(sqlCase).run();
    // }
    //window
    @Test(dataProvider = "getCase",dataProviderClass = BaseTest.class)
    @Yaml(filePaths ="integration_test/tmp/")
    @Story("api request")
     public void testBatch(SQLCase sqlCase){
        new RestfulOnlineExecutor(sqlCase).run();
    }

    // @Test(dataProvider = "getCase",dataProviderClass = BaseTest.class)
    // @Yaml(filePaths ="integration_test/cluster/")
    // @Story("api request")
    //  public void testBatch(SQLCase sqlCase){
    //     new RestfulOnlineExecutor(sqlCase).run();
    // }

    // @Test(dataProvider = "getCase",dataProviderClass = BaseTest.class)
    // @Yaml(filePaths ="integration_test/test_index_optimized.yaml/")
    // @Story("api request")
    //  public void testBatch(SQLCase sqlCase){
    //     new RestfulOnlineExecutor(sqlCase).run();
    // }

}