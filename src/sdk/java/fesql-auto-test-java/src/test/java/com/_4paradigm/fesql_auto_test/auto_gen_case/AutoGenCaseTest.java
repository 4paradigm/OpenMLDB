package com._4paradigm.fesql_auto_test.auto_gen_case;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlClient;
import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.entity.FesqlDataProviderList;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;
import com._4paradigm.fesql_auto_test.util.FEDBDeploy;
import com._4paradigm.sql.sdk.SqlExecutor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/12/28 1:05 PM
 */
// @Slf4j
public class AutoGenCaseTest extends FesqlTest {

    private Map<String,SqlExecutor> executorMap = new HashMap<>();
    private Map<String,FEDBInfo> fedbInfoMap = new HashMap<>();

    @BeforeClass
    public void beforeClass(){
        if(FesqlConfig.INIT_VERSION_ENV) {
            FesqlConfig.VERSIONS.forEach(version -> {
                FEDBDeploy fedbDeploy = new FEDBDeploy(version);
                FEDBInfo fedbInfo = fedbDeploy.deployFEDB(2, 3);
                FesqlClient fesqlClient = new FesqlClient(fedbInfo);
                executorMap.put(version, fesqlClient.getExecutor());
                fedbInfoMap.put(version, fedbInfo);
            });
            fedbInfoMap.put("mainVersion", FesqlConfig.mainInfo);
        }else{
            //测试调试用
            String verion = "2020-02-06";
            FEDBInfo fedbInfo = FEDBInfo.builder()
                    .zk_cluster("172.27.128.37:10006")
                    .zk_root_path("/fedb")
                    .tabletEndpoints(Lists.newArrayList("172.27.128.37:10011", "172.27.128.37:10012", "172.27.128.37:10013"))
                    .build();
            executorMap.put(verion, new FesqlClient(fedbInfo).getExecutor());
            fedbInfoMap.put(verion, fedbInfo);
            fedbInfoMap.put("mainVersion", FesqlConfig.mainInfo);
        }
    }

    @DataProvider()
    public Object[] getGenCaseData() throws FileNotFoundException {
        FesqlDataProviderList dp = FesqlDataProviderList
                .dataProviderGenerator(new String[]{"/auto_gen_cases"});
        return dp.getCases().toArray();
    }

    @Test(dataProvider = "getGenCaseData")
    public void testGenCaseBatch(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, executorMap, fedbInfoMap, testCase, ExecutorFactory.ExecutorType.kDiffBatch).run();
    }
    @Test(dataProvider = "getGenCaseData")
    public void testGenCaseRequestMode(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, executorMap, fedbInfoMap, testCase, ExecutorFactory.ExecutorType.kDiffRequest).run();
    }
    @Test(dataProvider = "getGenCaseData")
    public void testGenCaseRequestModeWithSp(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, executorMap, fedbInfoMap, testCase, ExecutorFactory.ExecutorType.kDiffRequestWithSp).run();
    }
    @Test(dataProvider = "getGenCaseData")
    public void testGenCaseRequestModeWithSpAsync(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, executorMap, fedbInfoMap, testCase, ExecutorFactory.ExecutorType.kDiffRequestWithSpAsync).run();
    }
}
