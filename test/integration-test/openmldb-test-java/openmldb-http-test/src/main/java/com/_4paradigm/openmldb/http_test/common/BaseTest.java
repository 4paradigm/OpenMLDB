package com._4paradigm.openmldb.http_test.common;


import com._4paradigm.openmldb.java_sdk_test.common.FedbClient;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCase;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCaseFile;
import com._4paradigm.openmldb.test_common.util.FEDBDeploy;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.util.List;

@Slf4j
public class BaseTest {
    protected Logger reportLog = new LogProxy(log);
    protected SqlExecutor executor;
    @DataProvider(name = "getCase")
    public Object[] getCaseByYaml(Method method) throws FileNotFoundException {
        String[] casePaths = method.getAnnotation(Yaml.class).filePaths();
        if (casePaths == null || casePaths.length == 0) {
            throw new RuntimeException("please add @Yaml");
        }
        List<RestfulCaseFile> caseFileList = RestfulCaseFileList.generatorCaseFileList(casePaths);
        List<RestfulCase> cases = RestfulCaseFileList.getCases(caseFileList);
        return cases.toArray();
    }

    @BeforeTest()
    @Parameters({"env", "version", "fedbPath"})
    public void beforeTest(@Optional("qa") String env, @Optional("main") String version, @Optional("") String fedbPath) throws Exception {
        RestfulGlobalVar.env = env;
        String caseEnv = System.getProperty("caseEnv");
        if (!StringUtils.isEmpty(caseEnv)) {
            RestfulGlobalVar.env = caseEnv;
        }
        log.info("fedb global var env: {}", RestfulGlobalVar.env);
        if (env.equalsIgnoreCase("cluster")) {
            FEDBDeploy fedbDeploy = new FEDBDeploy(version);
            fedbDeploy.setFedbPath(fedbPath);
            fedbDeploy.setCluster(true);
            RestfulGlobalVar.mainInfo = fedbDeploy.deployFEDB(2, 3);
        } else if (env.equalsIgnoreCase("standalone")) {
            FEDBDeploy fedbDeploy = new FEDBDeploy(version);
            fedbDeploy.setFedbPath(fedbPath);
            fedbDeploy.setCluster(false);
            RestfulGlobalVar.mainInfo = fedbDeploy.deployFEDB(2, 3);
        } else {
            RestfulGlobalVar.mainInfo = FEDBInfo.builder()
                    .basePath("/home/zhaowei01/fedb-auto-test/main")
                    .fedbPath("/home/zhaowei01/fedb-auto-test/main/fedb-ns-1/bin/fedb")
                    .zk_cluster("172.24.4.55:10000")
                    .zk_root_path("/fedb")
                    .nsNum(2).tabletNum(3)
                    .nsEndpoints(Lists.newArrayList("172.24.4.55:10001", "172.24.4.55:10002"))
                    .tabletEndpoints(Lists.newArrayList("172.24.4.55:10003", "172.24.4.55:10004", "172.24.4.55:10005"))
                    .apiServerEndpoints(Lists.newArrayList("172.24.4.55:20000"))
                    .build();
        }
        FedbClient fesqlClient = new FedbClient(RestfulGlobalVar.mainInfo);
        executor = fesqlClient.getExecutor();
        System.out.println("fesqlClient = " + fesqlClient);
    }
}
