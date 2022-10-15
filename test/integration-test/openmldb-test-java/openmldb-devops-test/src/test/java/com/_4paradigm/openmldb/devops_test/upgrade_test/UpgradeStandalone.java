package com._4paradigm.openmldb.devops_test.upgrade_test;

import com._4paradigm.openmldb.devops_test.common.ClusterTest;
import com._4paradigm.openmldb.devops_test.util.CheckUtil;
import com._4paradigm.openmldb.test_common.openmldb.NsClient;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBDevops;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
import com._4paradigm.qa.openmldb_deploy.util.DeployUtil;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class UpgradeStandalone extends ClusterTest {
    private String dbName;
    private String memoryTableName;
    private String ssdTableName;
    private String hddTableName;
    private SDKClient sdkClient;
    private NsClient nsClient;
    private OpenMLDBDevops openMLDBDevops;
    private String newBinPath;
    private String confPath;
    private String upgradePath;
    @BeforeClass
    @Parameters("upgradeVersion")
    public void beforeClass(@Optional("0.6.0") String upgradeVersion){
        dbName = "test_upgrade";
        memoryTableName = "test_memory";
        ssdTableName = "test_ssd";
        hddTableName = "test_hdd";
        sdkClient = SDKClient.of(executor);
        nsClient = NsClient.of(OpenMLDBGlobalVar.mainInfo);
        openMLDBDevops = OpenMLDBDevops.of(OpenMLDBGlobalVar.mainInfo,dbName);

        sdkClient.createAndUseDB(dbName);
        upgradePath = DeployUtil.getTestPath(version)+"/upgrade_"+upgradeVersion;
        File file = new File(upgradePath);
        if(!file.exists()){
            file.mkdirs();
        }
        OpenMLDBDeploy openMLDBDeploy = new OpenMLDBDeploy(upgradeVersion);
        String upgradeDirectoryName = openMLDBDeploy.downloadOpenMLDB(upgradePath);
        newBinPath = upgradeDirectoryName+"/bin/";
        confPath = upgradeDirectoryName+"/conf";
    }
    @Test
    public void testUpgrade(){
//        Map<String,List<Long>> map1 = nsClient.getTableOffset(dbName);
//        log.info("升级前offset："+map1);
        openMLDBDevops.upgradeStandalone(newBinPath,confPath);
//        Map<String,List<Long>> map2 = nsClient.getTableOffset(dbName);
//        log.info("升级后offset："+map2);
//        Assert.assertEquals(map1,map2);
        String memoryTableDDL = "create table test_memory(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=1,replicanum=1);";
        String ssdTableDDL = "create table test_ssd(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=1,replicanum=1,storage_mode=\"SSD\");";
        String hddTableDDL = "create table test_hdd(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=1,replicanum=1,storage_mode=\"HDD\");";
        // 插入一定量的数据
        int dataCount = 100;
        List<List<Object>> dataList = new ArrayList<>();
        for(int i=0;i<dataCount;i++){
            List<Object> list = Lists.newArrayList("aa" + i, 1, 2, 3, 1.1, 2.1, 1590738989000L, "2020-05-01", true);
            dataList.add(list);
        }
        sdkClient.execute(Lists.newArrayList(memoryTableDDL));
        sdkClient.insertList(memoryTableName,dataList);
        if(version.compareTo("0.5.0")>=0) {
            sdkClient.execute(Lists.newArrayList(ssdTableDDL, hddTableDDL));
            sdkClient.insertList(ssdTableName, dataList);
            sdkClient.insertList(hddTableName, dataList);
        }

        CheckUtil.addDataCheckByOffset(sdkClient, nsClient, dbName, Lists.newArrayList(memoryTableName), 100, 10);
        if(version.compareTo("0.5.0")>=0) {
            CheckUtil.addDataCheckByOffset(sdkClient, nsClient, dbName, Lists.newArrayList(ssdTableName, hddTableName), 100, 10);
        }
    }

//    @AfterClass
    public void afterClass(){
        String command = "rm -rf "+upgradePath;
        ExecutorUtil.run(command);
    }
}
