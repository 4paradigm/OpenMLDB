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

package com._4paradigm.openmldb.devops_test.common;


import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBClient;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.provider.YamlUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBDeployType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

import java.io.File;
import java.sql.Statement;

/**
 * @author zhaowei
 * @date 2020/6/11 2:02 PM
 */
@Slf4j
public class ClusterTest {
    protected static SqlExecutor executor;
    protected String version;
    protected String openMLDBPath;
    @BeforeTest()
    @Parameters({"env","version","openMLDBPath"})
    public void beforeTest(@Optional("qa") String env,@Optional("main") String version,@Optional("")String openMLDBPath) throws Exception {
        OpenMLDBGlobalVar.env = env;
        this.version = version;
        this.openMLDBPath = openMLDBPath;
        if(env.equalsIgnoreCase("cluster")){
            OpenMLDBDeploy openMLDBDeploy = new OpenMLDBDeploy(version);;
            openMLDBDeploy.setOpenMLDBPath(openMLDBPath);
            openMLDBDeploy.setCluster(true);
            OpenMLDBGlobalVar.mainInfo = openMLDBDeploy.deployCluster(2, 3);
        }else if(env.equalsIgnoreCase("single")){
            OpenMLDBDeploy openMLDBDeploy = new OpenMLDBDeploy(version);;
            openMLDBDeploy.setOpenMLDBPath(openMLDBPath);
            openMLDBDeploy.setCluster(true);
            openMLDBDeploy.setSystemTableReplicaNum(1);
            OpenMLDBGlobalVar.mainInfo = openMLDBDeploy.deployCluster(1, 1);
        }else{
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

            OpenMLDBGlobalVar.mainInfo = openMLDBInfo;
            OpenMLDBGlobalVar.env = "cluster";
        }
        File outFile = new File("out");
        if(!outFile.exists()){
            outFile.mkdir();
        }
        YamlUtil.writeYamlFile(OpenMLDBGlobalVar.mainInfo,"out/openmldb_info.yaml");
        String caseEnv = System.getProperty("caseEnv");
        if (!StringUtils.isEmpty(caseEnv)) {
            OpenMLDBGlobalVar.env = caseEnv;
        }
        log.info("openmldb global var env: {}", env);
        OpenMLDBClient openMLDBClient = new OpenMLDBClient(OpenMLDBGlobalVar.mainInfo.getZk_cluster(), OpenMLDBGlobalVar.mainInfo.getZk_root_path());
        executor = openMLDBClient.getExecutor();
        log.info("executor:{}",executor);
        Statement statement = executor.getStatement();
        statement.execute("SET @@execute_mode='online';");
    }
}
