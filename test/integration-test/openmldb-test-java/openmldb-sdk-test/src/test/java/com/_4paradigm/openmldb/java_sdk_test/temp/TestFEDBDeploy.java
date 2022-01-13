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

package com._4paradigm.openmldb.java_sdk_test.temp;

import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.util.FEDBDeploy;
import org.testng.annotations.Test;

public class TestFEDBDeploy {
    @Test
    public void test1(){
        FEDBDeploy deploy = new FEDBDeploy("0.2.3");
        FEDBInfo fedbInfo = deploy.deployFEDB(2,3);
        System.out.println(fedbInfo);
    }
    @Test
    public void test5(){
        FEDBDeploy deploy = new FEDBDeploy("0.2.3");
        deploy.setCluster(false);
        FEDBInfo fedbInfo = deploy.deployFEDB(2,3);
        System.out.println(fedbInfo);
    }
    @Test
    public void test3(){
        FEDBDeploy deploy = new FEDBDeploy("2.2.2");
        FEDBInfo fedbInfo = deploy.deployFEDB(2,3);
        System.out.println(fedbInfo);
    }
    @Test
    public void test2(){
        FEDBDeploy deploy = new FEDBDeploy("main");
        deploy.setCluster(false);
        FEDBInfo fedbInfo = deploy.deployFEDB(2, 3);
        System.out.println(fedbInfo);
    }

    @Test
    public void testTmp(){
        FEDBDeploy deploy = new FEDBDeploy("tmp");
        deploy.setCluster(true);
        deploy.setSparkMaster("local");
        // deploy.setBatchJobJarPath("hdfs://172.27.128.215:8020/Users/tobe/openmldb-batchjob-0.4.0-SNAPSHOT.jar");
        // deploy.setSparkYarnJars("hdfs://172.27.128.215:8020/Users/tobe/openmldb_040_jars/*");
        FEDBInfo fedbInfo = deploy.deployFEDB(2, 3);
        System.out.println(fedbInfo);
    }
    @Test
    public void testStandalone(){
        FEDBDeploy deploy = new FEDBDeploy("standalone");
        FEDBInfo fedbInfo = deploy.deployFEDBByStandalone();
        System.out.println(fedbInfo);
    }

    @Test
    public void testTask(){
        FEDBDeploy deploy = new FEDBDeploy("tmp");
        deploy.setFedbName("openmldb_linux");
        deploy.setCluster(true);
        deploy.setSparkMaster("local");
        deploy.deployTaskManager("/home/zhaowei01/fedb-auto-test/tmp","172.24.4.55",1,"172.24.4.55:10000");
        // System.out.println(fedbInfo);
    }

    @Test
    public void testLocalDeploy(){
        FEDBDeploy deploy = new FEDBDeploy("tmp");
        deploy.setCluster(true);
        deploy.setSparkMaster("local");
        // deploy.setBatchJobJarPath("hdfs://172.27.128.215:8020/Users/tobe/openmldb-batchjob-0.4.0-SNAPSHOT.jar");
        // deploy.setSparkYarnJars("hdfs://172.27.128.215:8020/Users/tobe/openmldb_040_jars/*");
        FEDBInfo fedbInfo = deploy.deployFEDB(2, 3);
        System.out.println(fedbInfo);
    }
}
