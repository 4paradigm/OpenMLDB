package com._4paradigm.fesql_auto_test.v1;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProviderList;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import io.qameta.allure.Feature;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
// @Slf4j
@Feature("Insert")
public class InsertTest extends FesqlTest {

    @DataProvider
    public Object[] testInsertData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/integration/v1/test_insert.yaml"});
        return dp.getCases().toArray();
    }
    @Test(dataProvider = "testInsertData")
    public void testInsert(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase, ExecutorFactory.ExecutorType.kDDL).run();
    }
}
