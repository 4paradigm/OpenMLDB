/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.java_sdk_test.auto_gen_case;

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBConfig;
import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBTest;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBClient;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBDeployType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
import com.google.common.collect.Lists;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/12/28 1:05 PM
 */
@Slf4j
@Feature("AutoCase")
public class AutoGenCaseTest extends OpenMLDBTest {

    private Map<String, SqlExecutor> executorMap = new HashMap<>();
    private Map<String, OpenMLDBInfo> fedbInfoMap = new HashMap<>();

    @BeforeClass
    public void beforeClass(){
        if(OpenMLDBConfig.INIT_VERSION_ENV) {
            OpenMLDBConfig.VERSIONS.forEach(version -> {
                OpenMLDBDeploy openMLDBDeploy = new OpenMLDBDeploy(version);
                openMLDBDeploy.setCluster("cluster".equals(OpenMLDBGlobalVar.env));
                OpenMLDBInfo fedbInfo = openMLDBDeploy.deployCluster(2, 3);
                OpenMLDBClient fesqlClient = new OpenMLDBClient(fedbInfo.getZk_cluster(),fedbInfo.getZk_root_path());
                executorMap.put(version, fesqlClient.getExecutor());
                fedbInfoMap.put(version, fedbInfo);
            });
            fedbInfoMap.put("mainVersion", OpenMLDBGlobalVar.mainInfo);
        }else{
            //测试调试用
            String verion = "2.2.2";
            OpenMLDBInfo openMLDBInfo = new OpenMLDBInfo();
            openMLDBInfo.setDeployType(OpenMLDBDeployType.CLUSTER);
            openMLDBInfo.setNsNum(2);
            openMLDBInfo.setTabletNum(3);
            openMLDBInfo.setBasePath("/home/zhaowei01/openmldb-auto-test/tmp");
            openMLDBInfo.setZk_cluster("172.24.4.55:30000");
            openMLDBInfo.setZk_root_path("/openmldb");
            openMLDBInfo.setNsEndpoints(Lists.newArrayList("172.24.4.55:30004", "172.24.4.55:30005"));
            openMLDBInfo.setNsNames(Lists.newArrayList());
            openMLDBInfo.setTabletEndpoints(Lists.newArrayList("172.24.4.55:30001", "172.24.4.55:30002", "172.24.4.55:30003"));
            openMLDBInfo.setTabletNames(Lists.newArrayList());
            openMLDBInfo.setApiServerEndpoints(Lists.newArrayList("172.24.4.55:30006"));
            openMLDBInfo.setApiServerNames(Lists.newArrayList());
            openMLDBInfo.setTaskManagerEndpoints(Lists.newArrayList("172.24.4.55:30007"));
            openMLDBInfo.setOpenMLDBPath("/home/zhaowei01/openmldb-auto-test/tmp/openmldb-ns-1/bin/openmldb");

            executorMap.put(verion, new OpenMLDBClient(openMLDBInfo.getZk_cluster(),openMLDBInfo.getZk_root_path()).getExecutor());
            fedbInfoMap.put(verion, openMLDBInfo);
            fedbInfoMap.put("mainVersion", OpenMLDBGlobalVar.mainInfo);
        }
    }

//    @DataProvider()
//    public Object[] getGenCaseData() throws FileNotFoundException {
//        FesqlDataProviderList dp = FesqlDataProviderList
//                .dataProviderGenerator(new String[]{"/auto_gen_cases"});
//        return dp.getCases().toArray();
//    }

    @Story("batch")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "hybridsql_gen_cases/")
    public void testGenCaseBatch(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, executorMap, fedbInfoMap, testCase, SQLCaseType.kDiffBatch).run();
    }
    @Story("request")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "hybridsql_gen_cases/")
    public void testGenCaseRequestMode(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, executorMap, fedbInfoMap, testCase, SQLCaseType.kDiffRequest).run();
    }
    @Story("requestWithSp")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "hybridsql_gen_cases/")
    public void testGenCaseRequestModeWithSp(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, executorMap, fedbInfoMap, testCase, SQLCaseType.kDiffRequestWithSp).run();
    }
    @Story("requestWithSpAysn")
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "hybridsql_gen_cases/")
    public void testGenCaseRequestModeWithSpAsync(SQLCase testCase) throws Exception {
        ExecutorFactory.build(executor, executorMap, fedbInfoMap, testCase, SQLCaseType.kDiffRequestWithSpAsync).run();
    }
}
