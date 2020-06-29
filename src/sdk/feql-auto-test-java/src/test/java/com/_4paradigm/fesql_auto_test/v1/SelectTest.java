package com._4paradigm.fesql_auto_test.v1;

import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlCase;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProvider;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
@Slf4j
public class SelectTest extends FesqlTest {

    @DataProvider
    public Object[] testSampleSelectData() throws FileNotFoundException {
        FesqlDataProvider dp = FesqlDataProvider
                .dataProviderGenerator("/v1/testSelect-sample.yaml");
        return dp.getCases();
    }

    @Test(dataProvider = "testSampleSelectData")
    public void testSampleSelect(FesqlCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase).run();
    }
    @DataProvider
    public Object[] testExpressionData() throws FileNotFoundException {
        FesqlDataProvider dp = FesqlDataProvider
                .dataProviderGenerator("/v1/testExpression.yaml");
        return dp.getCases();
    }

    @Test(dataProvider = "testExpressionData")
    public void testExpression(FesqlCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase).run();
    }
    @DataProvider
    public Object[] testGroupFunctionData() throws FileNotFoundException {
        FesqlDataProvider dp = FesqlDataProvider
                .dataProviderGenerator("/v1/testGroupFunction.yaml");
        return dp.getCases();
    }

    @Test(dataProvider = "testGroupFunctionData")
    public void testGroupFunction(FesqlCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase).run();
    }
    @DataProvider
    public Object[] testSubSelectData() throws FileNotFoundException {
        FesqlDataProvider dp = FesqlDataProvider
                .dataProviderGenerator("/v1/testSubSelect.yaml");
        return dp.getCases();
    }

    @Test(dataProvider = "testSubSelectData")
    public void testSubSelect(FesqlCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase).run();
    }
}
