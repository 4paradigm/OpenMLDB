package com._4paradigm.openmldb.devops_test.node_expansion;

import com._4paradigm.openmldb.devops_test.common.ClusterTest;
import com._4paradigm.openmldb.test_common.openmldb.NsClient;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBDevops;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
import com._4paradigm.test_tool.command_tool.common.LinuxUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestCluster extends ClusterTest {
    private String dbName;
    private SDKClient sdkClient;
    private NsClient nsClient;
    private OpenMLDBDevops openMLDBDevops;
    @BeforeClass
    public void beforeClass(){
        dbName = "test_devops1";
        sdkClient = SDKClient.of(executor);
        nsClient = NsClient.of(OpenMLDBGlobalVar.mainInfo);
        openMLDBDevops = OpenMLDBDevops.of(OpenMLDBGlobalVar.mainInfo,dbName);
        OpenMLDBDeploy deploy = new OpenMLDBDeploy(version);
        deploy.setOpenMLDBPath(openMLDBPath);
        deploy.setOpenMLDBDirectoryName(OpenMLDBGlobalVar.mainInfo.getOpenMLDBDirectoryName());
        String zk_cluster = OpenMLDBGlobalVar.mainInfo.getZk_cluster();
        String basePath = OpenMLDBGlobalVar.mainInfo.getBasePath();
        String ip = LinuxUtil.hostnameI();
        int port = deploy.deployTablet(basePath, ip, 4, zk_cluster, null);
        String tabletEndpoint = ip+":"+port;

    }
    @Test
    public void testAddTablet(){

    }
}
