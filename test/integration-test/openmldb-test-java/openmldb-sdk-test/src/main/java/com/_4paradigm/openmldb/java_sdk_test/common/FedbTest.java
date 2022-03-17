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

package com._4paradigm.openmldb.java_sdk_test.common;


import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBDeployType;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.util.FEDBDeploy;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @author zhaowei
 * @date 2020/6/11 2:02 PM
 */
@Slf4j
public class FedbTest extends BaseTest {
    protected static SqlExecutor executor;

    @BeforeTest()
    @Parameters({"env","version","fedbPath"})
    public void beforeTest(@Optional("qa") String env,@Optional("main") String version,@Optional("")String fedbPath) throws Exception {
        FedbGlobalVar.env = env;
        if(env.equalsIgnoreCase("cluster")){
            FEDBDeploy fedbDeploy = new FEDBDeploy(version);;
            fedbDeploy.setFedbPath(fedbPath);
            fedbDeploy.setCluster(true);
            FedbGlobalVar.mainInfo = fedbDeploy.deployFEDB(2, 3);
        }else if(env.equalsIgnoreCase("standalone")){
            FEDBDeploy fedbDeploy = new FEDBDeploy(version);
            fedbDeploy.setFedbPath(fedbPath);
            fedbDeploy.setCluster(false);
            FedbGlobalVar.mainInfo = fedbDeploy.deployFEDB(2, 3);
        }else{
            FedbGlobalVar.mainInfo = FEDBInfo.builder()
                    .deployType(OpenMLDBDeployType.CLUSTER)
                    .basePath("/home/zhaowei01/fedb-auto-test/tmp")
                    .fedbPath("/home/zhaowei01/fedb-auto-test/tmp/openmldb-ns-1/bin/openmldb")
                    .zk_cluster("172.24.4.55:10000")
                    .zk_root_path("/openmldb")
                    .nsNum(2).tabletNum(3)
                    .nsEndpoints(Lists.newArrayList("172.24.4.55:10004", "172.24.4.55:10005"))
                    .tabletEndpoints(Lists.newArrayList("172.24.4.55:10001", "172.24.4.55:10002", "172.24.4.55:10003"))
                    .apiServerEndpoints(Lists.newArrayList("172.24.4.55:10006"))
                    .build();
            FedbGlobalVar.env = "cluster";

        }
        String caseEnv = System.getProperty("caseEnv");
        if (!StringUtils.isEmpty(caseEnv)) {
            FedbGlobalVar.env = caseEnv;
        }
        log.info("fedb global var env: {}", env);
        FedbClient fesqlClient = new FedbClient(FedbGlobalVar.mainInfo);
        executor = fesqlClient.getExecutor();
        log.info("executor:{}",executor);
    }
}
