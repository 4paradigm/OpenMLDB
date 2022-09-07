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
package com._4paradigm.openmldb.http_test.common;


import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBDeployType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

@Slf4j
public class StandaloneTest extends BaseTest{

    @BeforeTest()
    @Parameters({"env", "version", "fedbPath"})
    public void beforeTest(@Optional("qa") String env, @Optional("main") String version, @Optional("") String fedbPath) throws Exception {
        RestfulGlobalVar.env = env;
        String caseEnv = System.getProperty("caseEnv");
        if (!StringUtils.isEmpty(caseEnv)) {
            RestfulGlobalVar.env = caseEnv;
        }
        log.info("openmldb global var env: {}", RestfulGlobalVar.env);
        if(env.equalsIgnoreCase("standalone")){
            OpenMLDBDeploy fedbDeploy = new OpenMLDBDeploy(version);
            fedbDeploy.setOpenMLDBPath(fedbPath);
            RestfulGlobalVar.mainInfo = fedbDeploy.deployStandalone();
        }else{
            OpenMLDBInfo openMLDBInfo = new OpenMLDBInfo();
            openMLDBInfo.setDeployType(OpenMLDBDeployType.STANDALONE);
            openMLDBInfo.setHost("172.24.4.55");
            openMLDBInfo.setPort(30013);
            openMLDBInfo.setNsNum(1);
            openMLDBInfo.setTabletNum(1);
            openMLDBInfo.setBasePath("/home/wangkaidong/fedb-auto-test/standalone");
            openMLDBInfo.setZk_cluster("172.24.4.55:30000");
            openMLDBInfo.setZk_root_path("/openmldb");
            openMLDBInfo.setNsEndpoints(Lists.newArrayList("172.24.4.55:30013"));
            openMLDBInfo.setTabletEndpoints(Lists.newArrayList("172.24.4.55:30014"));
            openMLDBInfo.setApiServerEndpoints(Lists.newArrayList("172.24.4.55:30015"));
            openMLDBInfo.setOpenMLDBPath("/home/wangkaidong/fedb-auto-test/standalone/openmldb-standalone/bin/openmldb");

            RestfulGlobalVar.mainInfo = openMLDBInfo;
        }
    }
}
