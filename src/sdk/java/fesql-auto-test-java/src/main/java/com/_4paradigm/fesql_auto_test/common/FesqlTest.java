package com._4paradigm.fesql_auto_test.common;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.sql.sdk.SqlExecutor;
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
public class FesqlTest implements ITest {
    protected static SqlExecutor executor;
    private ThreadLocal<String> testName = new ThreadLocal<>();

    @BeforeMethod
    public void BeforeMethod(Method method, Object[] testData) {
        if (testData[0] instanceof SQLCase) {
            SQLCase sqlCase = (SQLCase) testData[0];
            testName.set(String.format("%s_%s_%s",
                    method.getName(), sqlCase.getId(), sqlCase.getDesc()));
        } else {
            testName.set(method.getName() + "_" + testData[0]);
        }
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
        FesqlClient fesqlClient = new FesqlClient(FesqlConfig.ZK_CLUSTER, FesqlConfig.ZK_ROOT_PATH);
        executor = fesqlClient.getExecutor();
    }
}
