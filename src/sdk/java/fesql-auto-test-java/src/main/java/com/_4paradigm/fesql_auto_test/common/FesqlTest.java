package com._4paradigm.fesql_auto_test.common;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProvider;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.ITest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

import java.lang.reflect.Method;
import java.util.ArrayList;

/**
 * @author zhaowei
 * @date 2020/6/11 2:02 PM
 */
@Slf4j
public class FesqlTest implements ITest {
    protected static SqlExecutor executor;
    private ThreadLocal<String> testName = new ThreadLocal<>();
    private int testNum = 0;

    public static String CaseNameFormat(SQLCase sqlCase) {
        return String.format("%s_%s_%s",
                FesqlGlobalVar.env, sqlCase.getId(), sqlCase.getDesc());
    }

    @BeforeMethod
    public void BeforeMethod(Method method, Object[] testData) {
        Assert.assertNotNull(
                testData[0], "fail to run fesql test with null SQLCase: check yaml case");
        if (testData[0] instanceof SQLCase) {
            SQLCase sqlCase = (SQLCase) testData[0];
            Assert.assertNotEquals(FesqlDataProvider.FAIL_SQL_CASE,
                    sqlCase.getDesc(), "fail to run fesql test with FAIL DATA PROVIDER SQLCase: check yaml case");
            testName.set(String.format("[%d]%s.%s", testNum, method.getName(), CaseNameFormat(sqlCase)));
        } else {
            testName.set(String.format("[%d]%s.%s", testNum, method.getName(), null == testData[0] ? "null" : testData[0].toString()));
        }
        testNum++;
    }

    protected ArrayList<String> tableNameList = new ArrayList<>();
    protected String tableNamePrefix = "auto_";

    @Override
    public String getTestName() {
        return testName.get();
    }

    @BeforeTest()
    @Parameters({"env"})
    public void beforeTest(@Optional("qa") String env) throws Exception {
        FesqlGlobalVar.env = env;
        String caseEnv = System.getProperty("caseEnv");
        if (!StringUtils.isEmpty(caseEnv)) {
            FesqlGlobalVar.env = caseEnv;
        }
        log.info("fesql global var env: {}", env);
        FesqlClient fesqlClient = new FesqlClient(FesqlConfig.ZK_CLUSTER, FesqlConfig.ZK_ROOT_PATH);
        executor = fesqlClient.getExecutor();
    }
}
