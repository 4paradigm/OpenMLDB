package com._4paradigm.openmldb.devops_test.upgrade_test;

import com._4paradigm.openmldb.devops_test.common.ClusterTest;
import com._4paradigm.openmldb.devops_test.util.CheckUtil;
import com._4paradigm.openmldb.test_common.openmldb.*;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
import com._4paradigm.qa.openmldb_deploy.util.DeployUtil;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class UpgradeClusterByCLI extends ClusterTest {
    private String dbName;
    private String memoryTableName;
    private String ssdTableName;
    private String hddTableName;
    private CliClient cliClient;
    private NsClient nsClient;
    private OpenMLDBDevops openMLDBDevops;
    private String openMLDBPath;
    private SDKClient sdkClient;
    private String newBinPath;
    private String confPath;
    private String upgradePath;
    private OpenMLDBDeploy openMLDBDeploy;
    private String upgradeVersion;
    private String upgradeDirectoryName;
    @BeforeClass
    @Parameters("upgradeVersion")
    public void beforeClass(@Optional("0.6.0") String upgradeVersion){
        this.upgradeVersion = upgradeVersion;
        dbName = "test_upgrade";
        memoryTableName = "test_memory";
        ssdTableName = "test_ssd";
        hddTableName = "test_hdd";
        cliClient = CliClient.of(OpenMLDBGlobalVar.mainInfo,dbName);
        nsClient = NsClient.of(OpenMLDBGlobalVar.mainInfo);
        openMLDBDevops = OpenMLDBDevops.of(OpenMLDBGlobalVar.mainInfo,dbName);
        cliClient.setGlobalOnline();
        int dataCount = 100;
        cliClient.create(dbName);
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
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=3);";
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
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=3,storage_mode=\"SSD\");";
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
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=3,storage_mode=\"HDD\");";
        List<List<Object>> dataList = new ArrayList<>();
        for(int i=0;i<dataCount;i++){
            List<Object> list = Lists.newArrayList("aa" + i, 1, 2, 3, 1.1, 2.1, 1590738989000L, "2020-05-01", true);
            dataList.add(list);
        }
        cliClient.execute(Lists.newArrayList(memoryTableDDL));
        cliClient.insertList(memoryTableName,dataList);
        if(version.compareTo("0.5.0")>=0) {
            cliClient.execute(Lists.newArrayList(ssdTableDDL, hddTableDDL));
            cliClient.insertList(ssdTableName, dataList);
            cliClient.insertList(hddTableName, dataList);
        }
        upgradePath = DeployUtil.getTestPath(version)+"/upgrade_"+upgradeVersion;
        File file = new File(upgradePath);
        if(!file.exists()){
            file.mkdirs();
        }
        openMLDBDeploy = new OpenMLDBDeploy(upgradeVersion);
        upgradeDirectoryName = openMLDBDeploy.downloadOpenMLDB(upgradePath);
        openMLDBPath = upgradePath+"/"+upgradeDirectoryName+"/bin/openmldb";
        newBinPath = upgradePath+"/"+upgradeDirectoryName+"/bin/";
        confPath = upgradePath+"/"+upgradeDirectoryName+"/conf";
    }
    @Test
    public void testUpgrade(){
        Map<String,List<Long>> beforeMap;
        if(version.compareTo("0.6.0")>=0){
            beforeMap = nsClient.getTableOffset(dbName);
        }else{
            beforeMap = cliClient.showTableStatus();
        }
        log.info("升级前offset："+beforeMap);
        openMLDBDevops.upgradeNs(newBinPath,confPath);
        openMLDBDevops.upgradeTablet(newBinPath,confPath);
        openMLDBDevops.upgradeApiServer(newBinPath,confPath);
        ExecutorUtil.run("cp -r " + upgradePath+"/"+upgradeDirectoryName + " " + OpenMLDBGlobalVar.mainInfo.getBasePath());
        openMLDBDevops.upgradeTaskManager(openMLDBDeploy);
        Map<String,List<Long>> afterMap;
        if(version.compareTo("0.6.0")>=0){
            afterMap = nsClient.getTableOffset(dbName);
        }else{
            afterMap = cliClient.showTableStatus();
        }
        log.info("升级后offset："+afterMap);
        Assert.assertEquals(beforeMap,afterMap);
        sdkClient = SDKClient.of(executor);
        sdkClient.useDB(dbName);
        if(upgradeVersion.compareTo("0.6.0")>=0) {
            if(version.compareTo("0.5.0")>=0) {
                CheckUtil.addDataCheckByOffset(sdkClient, nsClient, dbName, Lists.newArrayList(memoryTableName, ssdTableName, hddTableName), 100, 10);
            }else{
                CheckUtil.addDataCheckByOffset(sdkClient, nsClient, dbName, Lists.newArrayList(memoryTableName), 100, 10);
            }
        }else{
            if(version.compareTo("0.5.0")>=0) {
                CheckUtil.addDataCheckByCount(sdkClient, Lists.newArrayList(memoryTableName, ssdTableName, hddTableName), 100, 10);
            }else{
                CheckUtil.addDataCheckByCount(sdkClient, Lists.newArrayList(memoryTableName), 100, 10);
            }
        }
    }

//    @AfterClass
    public void afterClass(){
        String command = "rm -rf "+upgradePath;
        ExecutorUtil.run(command);
    }
}
