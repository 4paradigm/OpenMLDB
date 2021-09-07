package com._4paradigm.openmldb.java_sdk_test.diff_test;

import com._4paradigm.openmldb.java_sdk_test.common.FedbTest;
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
 * @date 2021/3/13 1:17 PM
 */
@Slf4j
@Feature("diff sql result")
public class DiffResultTest extends FedbTest {
    @DataProvider()
    public Object[] getCreateData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/integration/v1/test_create.yaml"});
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getCreateData")
    @Story("create")
    public void testCreate(SQLCase testCase){
        ExecutorFactory.build(executor, testCase, SQLCaseType.kDiffSQLResult).run();
    }

}
