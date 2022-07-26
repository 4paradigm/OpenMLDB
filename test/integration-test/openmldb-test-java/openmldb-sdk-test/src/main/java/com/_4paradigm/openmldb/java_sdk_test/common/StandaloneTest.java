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
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBClient;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBDeployType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
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
public class StandaloneTest extends BaseTest {
    protected static SqlExecutor executor;

    @BeforeTest()
    @Parameters({"env","version","openMLDBPath"})
    public void beforeTest(@Optional("qa") String env,@Optional("main") String version,@Optional("")String openMLDBPath) throws Exception {
        OpenMLDBGlobalVar.env = env;
        if(env.equalsIgnoreCase("standalone")){
            OpenMLDBDeploy openMLDBDeploy = new OpenMLDBDeploy(version);
            openMLDBDeploy.setOpenMLDBPath(openMLDBPath);
            OpenMLDBGlobalVar.mainInfo = openMLDBDeploy.deployStandalone();
        }else{
            OpenMLDBGlobalVar.mainInfo = OpenMLDBInfo.builder()
                    .deployType(OpenMLDBDeployType.STANDALONE)
                    .basePath("/home/wangkaidong/fedb-auto-test/standalone")
                    .openMLDBPath("/home/wangkaidong/fedb-auto-test/standalone/openmldb-standalone/bin/openmldb")
                    .nsNum(1).tabletNum(1)
                    .nsEndpoints(Lists.newArrayList("172.24.4.55:30013"))
                    .tabletEndpoints(Lists.newArrayList("172.24.4.55:30014"))
                    .apiServerEndpoints(Lists.newArrayList("172.24.4.55:30015"))
                    .host("172.24.4.55")
                    .port(30013)
                    .build();
        }
        String caseEnv = System.getProperty("caseEnv");
        if (!StringUtils.isEmpty(caseEnv)) {
            OpenMLDBGlobalVar.env = caseEnv;
        }
        //单机版SDK
        OpenMLDBClient standaloneClient = new OpenMLDBClient(OpenMLDBGlobalVar.mainInfo.getHost(), OpenMLDBGlobalVar.mainInfo.getPort());
        executor = standaloneClient.getExecutor();
        log.info("executor : {}",executor);
        log.info("fedb global var env: {}", env);
    }
}
