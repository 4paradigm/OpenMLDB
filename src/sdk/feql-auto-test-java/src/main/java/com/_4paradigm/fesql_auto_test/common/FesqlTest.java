package com._4paradigm.fesql_auto_test.common;

import com._4paradigm.sql.sdk.SqlExecutor;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

import java.util.ArrayList;

/**
 * @author zhaowei
 * @date 2020/6/11 2:02 PM
 */
public class FesqlTest {
    protected static SqlExecutor executor;

    protected ArrayList<String> tableNameList = new ArrayList<>();
    protected String tableNamePrefix = "auto_";

    @BeforeTest()
    @Parameters({"env"})
    public void beforeTest(@Optional("qa") String env) throws Exception {
        FesqlGlobalVar.env = env;
        FesqlClient fesqlClient = new FesqlClient(FesqlConfig.ZK_CLUSTER,FesqlConfig.ZK_ROOT_PATH);
        executor = fesqlClient.getExecutor();
    }
}
