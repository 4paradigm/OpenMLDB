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
package com._4paradigm.openmldb.java_sdk_test.diff_test;

import com._4paradigm.openmldb.java_sdk_test.common.JDBCTest;
import com._4paradigm.openmldb.test_common.model.OpenMLDBCaseFileList;
import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
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
@Feature("mysql")
public class MysqlTest extends JDBCTest {

    @DataProvider()
    public Object[] getCreateData() throws FileNotFoundException {
        OpenMLDBCaseFileList dp = OpenMLDBCaseFileList
                .dataProviderGenerator(new String[]{"/integration/v1/test_create.yaml"});
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getCreateData")
    @Story("create")
    public void testCreate(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kMYSQL).run();
    }

    @DataProvider()
    public Object[] getInsertData() throws FileNotFoundException {
        OpenMLDBCaseFileList dp = OpenMLDBCaseFileList
                .dataProviderGenerator(new String[]{"/integration/v1/test_insert.yaml"});
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getInsertData")
    @Story("insert")
    public void testInsert(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kMYSQL).run();
    }

    @DataProvider()
    public Object[] getSelectData() throws FileNotFoundException {
        OpenMLDBCaseFileList dp = OpenMLDBCaseFileList
                .dataProviderGenerator(new String[]{
                        "/integration/v1/select/test_select_sample.yaml",
                        "/integration/v1/select/test_sub_select.yaml"
                });
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getSelectData")
    @Story("select")
    public void testSelect(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kMYSQL).run();
    }

    @DataProvider()
    public Object[] getFunctionData() throws FileNotFoundException {
        OpenMLDBCaseFileList dp = OpenMLDBCaseFileList
                .dataProviderGenerator(new String[]{
                        "/integration/v1/function/",
                });
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getFunctionData")
    @Story("function")
    public void testFunction(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kMYSQL).run();
    }

    @DataProvider()
    public Object[] getExpressionData() throws FileNotFoundException {
        OpenMLDBCaseFileList dp = OpenMLDBCaseFileList
                .dataProviderGenerator(new String[]{
                        "/integration/v1/expression/",
                });
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getExpressionData")
    @Story("expression")
    public void testExpression(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kMYSQL).run();
    }

    // @DataProvider()
    // public Object[] getWindowData() throws FileNotFoundException {
    //     FesqlDataProviderList dp = FesqlDataProviderList
    //             .dataProviderGenerator(new String[]{
    //                     "/integration/v1/window/test_window.yaml",
    //             });
    //     return dp.getCases().toArray();
    // }
    //
    // @Test(dataProvider = "getWindowData")
    // @Story("window")
    // public void testWindow(SQLCase testCase){
    //     ExecutorFactory.build(testCase, SQLCaseType.kMYSQL).run();
    // }

}
