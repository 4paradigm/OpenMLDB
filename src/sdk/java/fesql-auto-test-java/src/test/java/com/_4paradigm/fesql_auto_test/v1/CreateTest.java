package com._4paradigm.fesql_auto_test.v1;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql.sqlcase.model.SQLCaseType;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProviderList;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import io.qameta.allure.Feature;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
@Slf4j
@Feature("Create")
public class CreateTest extends FesqlTest {

    @DataProvider()
    public Object[] getCreateData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/integration/v1/test_create.yaml"});
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getCreateData")
    public void testCreate(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kDDL).run();
    }

}
