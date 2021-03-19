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

import com._4paradigm.hyhridse.sqlcase.model.SQLCase;
import com._4paradigm.hyhridse.sqlcase.model.SQLCaseType;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProviderList;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import io.qameta.allure.Feature;
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
@Feature("DDL")
public class DDLTest extends FesqlTest {

    @DataProvider()
    public Object[] getCreateData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/integration/v1/test_create.yaml"});
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getCreateData")
    @Story("create")
    public void testCreate(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kDDL).run();
    }

    @DataProvider()
    public Object[] getInsertData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/integration/v1/test_insert.yaml"});
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getInsertData")
    @Story("insert")
    public void testInsert(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kDDL).run();
    }

    @DataProvider()
    public Object[] getTTLData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/integration/v1/ddl/test_ttl.yaml"});
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getTTLData")
    @Story("ttl")
    public void testTTL(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kDDL).run();
    }

}
