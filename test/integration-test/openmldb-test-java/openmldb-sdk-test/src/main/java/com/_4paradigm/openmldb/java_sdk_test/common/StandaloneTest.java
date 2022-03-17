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
public class StandaloneTest extends BaseTest {
    // protected static SqlExecutor executor;

    @BeforeTest()
    @Parameters({"env","version","fedbPath"})
    public void beforeTest(@Optional("qa") String env,@Optional("main") String version,@Optional("")String fedbPath) throws Exception {
        FedbGlobalVar.env = env;
        if(env.equalsIgnoreCase("standalone")){
            FEDBDeploy fedbDeploy = new FEDBDeploy(version);
            fedbDeploy.setFedbPath(fedbPath);
            FedbGlobalVar.mainInfo = fedbDeploy.deployFEDBByStandalone();
        }else{
            FedbGlobalVar.mainInfo = FEDBInfo.builder()
                    .deployType(OpenMLDBDeployType.STANDALONE)
                    .basePath("/home/zhaowei01/fedb-auto-test/standalone")
                    .fedbPath("/home/zhaowei01/fedb-auto-test/standalone/openmldb-standalone/bin/openmldb")
                    .nsNum(1).tabletNum(1)
                    .nsEndpoints(Lists.newArrayList("172.24.4.55:10019"))
                    .tabletEndpoints(Lists.newArrayList("172.24.4.55:10020"))
                    .apiServerEndpoints(Lists.newArrayList("172.24.4.55:10021"))
                    .host("172.24.4.55")
                    .port(10019)
                    .build();
        }
        String caseEnv = System.getProperty("caseEnv");
        if (!StringUtils.isEmpty(caseEnv)) {
            FedbGlobalVar.env = caseEnv;
        }
        log.info("fedb global var env: {}", env);
    }
}
