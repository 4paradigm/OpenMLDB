package com._4paradigm.openmldb.java_sdk_test.diff_test;


import com._4paradigm.openmldb.java_sdk_test.common.JDBCTest;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlDataProviderList;
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
@Feature("sqlite3")
public class Sqlite3Test extends JDBCTest {

    @DataProvider()
    public Object[] getCreateData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/integration/v1/test_create.yaml"});
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getCreateData")
    @Story("create")
    public void testCreate(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kSQLITE3).run();
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
        ExecutorFactory.build(testCase, SQLCaseType.kSQLITE3).run();
    }

    @DataProvider()
    public Object[] getSelectData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{
                        "/integration/v1/select/test_select_sample.yaml",
                        "/integration/v1/select/test_sub_select.yaml"
                });
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getSelectData")
    @Story("select")
    public void testSelect(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kSQLITE3).run();
    }

    @DataProvider()
    public Object[] getFunctionData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{
                        "/integration/v1/function/",
                });
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getFunctionData")
    @Story("function")
    public void testFunction(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kSQLITE3).run();
    }

    @DataProvider()
    public Object[] getExpressionData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{
                        "/integration/v1/expression/",
                });
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getExpressionData")
    @Story("expression")
    public void testExpression(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kSQLITE3).run();
    }

}
