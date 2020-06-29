package com._4paradigm.fesql_auto_test.v1;

import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FesqlCase;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProvider;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import com._4paradigm.sql.ResultSet;
import com._4paradigm.sql.Schema;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
@Slf4j
public class InsertTest extends FesqlTest {

    @DataProvider
    public Object[] testInsertData() throws FileNotFoundException {
        FesqlDataProvider dp = FesqlDataProvider
                .dataProviderGenerator("/v1/testInsert.yaml");
        return dp.getCases();
    }

    @Test(dataProvider = "testInsertData")
    public void testInsert(FesqlCase testCase) throws Exception {
        ExecutorFactory.build(executor,testCase).run();
    }
}
