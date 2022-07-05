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


import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBClient;
import com._4paradigm.openmldb.java_sdk_test.common.FedbGlobalVar;
import com._4paradigm.openmldb.sdk.SqlExecutor;
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
public class ClusterTest extends BaseTest{
    protected SqlExecutor executor;

    @BeforeTest()
    @Parameters({"env", "version", "fedbPath"})
    public void beforeTest(@Optional("qa") String env, @Optional("main") String version, @Optional("") String openMLDBPath) throws Exception {
        RestfulGlobalVar.env = env;
        String caseEnv = System.getProperty("caseEnv");
        if (!StringUtils.isEmpty(caseEnv)) {
            RestfulGlobalVar.env = caseEnv;
        }
        log.info("fedb global var env: {}", RestfulGlobalVar.env);
        if (env.equalsIgnoreCase("cluster")) {
            OpenMLDBDeploy openMLDBDeploy = new OpenMLDBDeploy(version);
            openMLDBDeploy.setOpenMLDBPath(openMLDBPath);
            openMLDBDeploy.setCluster(true);
            RestfulGlobalVar.mainInfo = openMLDBDeploy.deployCluster(2, 3);
        } else if (env.equalsIgnoreCase("standalone")) {
            OpenMLDBDeploy openMLDBDeploy = new OpenMLDBDeploy(version);
            openMLDBDeploy.setOpenMLDBPath(openMLDBPath);
            openMLDBDeploy.setCluster(false);
            RestfulGlobalVar.mainInfo = openMLDBDeploy.deployCluster(2, 3);
        } else {
            RestfulGlobalVar.mainInfo = OpenMLDBInfo.builder()
                    .deployType(OpenMLDBDeployType.CLUSTER)
                    .basePath("/home/zhaowei01/fedb-auto-test/tmp")
                    .openMLDBPath("/home/zhaowei01/fedb-auto-test/tmp/openmldb-ns-1/bin/openmldb")
                    .zk_cluster("172.24.4.55:30000")
                    .zk_root_path("/openmldb")
                    .nsNum(2).tabletNum(3)
                    .nsEndpoints(Lists.newArrayList("172.24.4.55:30004", "172.24.4.55:30005"))
                    .tabletEndpoints(Lists.newArrayList("172.24.4.55:30001", "172.24.4.55:30002", "172.24.4.55:30003"))
                    .apiServerEndpoints(Lists.newArrayList("172.24.4.55:30006"))
                    .build();
            FedbGlobalVar.env = "cluster";
        }
        OpenMLDBClient fesqlClient = new OpenMLDBClient(RestfulGlobalVar.mainInfo);
        executor = fesqlClient.getExecutor();
        System.out.println("fesqlClient = " + fesqlClient);
    }
}
