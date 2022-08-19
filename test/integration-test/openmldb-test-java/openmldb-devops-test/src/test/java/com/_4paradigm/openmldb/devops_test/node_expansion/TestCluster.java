package com._4paradigm.openmldb.devops_test.node_expansion;

import com._4paradigm.openmldb.devops_test.common.ClusterTest;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.openmldb.NsClient;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBDevops;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
import com._4paradigm.qa.openmldb_deploy.util.Tool;
import com._4paradigm.test_tool.command_tool.common.LinuxUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.util.ArrayList;
import java.util.List;

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
    }
    @Test
    public void testAddTablet(){
        String memoryTable = "test_memory";
        String ssdTable = "test_ssd";
        String hddTable = "test_hdd";
        // 创建磁盘表和内存表。
        int dataCount = 100;
        sdkClient.createAndUseDB(dbName);
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
        sdkClient.execute(Lists.newArrayList(memoryTableDDL,ssdTableDDL,hddTableDDL));
        // 插入一定量的数据
        List<List<Object>> dataList = new ArrayList<>();
        for(int i=0;i<dataCount;i++){
            List<Object> list = Lists.newArrayList("aa" + i, 1, 2, 3, 1.1, 2.1, 1590738989000L, "2020-05-01", true);
            dataList.add(list);
        }
        sdkClient.insertList(memoryTable,dataList);
        sdkClient.insertList(ssdTable,dataList);
        sdkClient.insertList(hddTable,dataList);
        // 增加一个tablet，数据可以正常访问。
        OpenMLDBDeploy deploy = new OpenMLDBDeploy(version);
        deploy.setOpenMLDBPath(openMLDBPath);
        deploy.setOpenMLDBDirectoryName(OpenMLDBGlobalVar.mainInfo.getOpenMLDBDirectoryName());
        String zk_cluster = OpenMLDBGlobalVar.mainInfo.getZk_cluster();
        String basePath = OpenMLDBGlobalVar.mainInfo.getBasePath();
        String ip = LinuxUtil.hostnameI();
        int port = deploy.deployTablet(basePath, ip, 4, zk_cluster, null);
        String addTabletEndpoint = ip+":"+port;
        sdkClient.checkComponentStatus(addTabletEndpoint, "online");
        addDataCheck(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount,0);
        // 可以创建四个副本的表，可以成功。
        String memoryTable4 = "test_memory4";
        String ssdTable4 = "test_ssd4";
        String hddTable4 = "test_hdd4";
        // 创建磁盘表和内存表。
        sdkClient.createAndUseDB(dbName);
        String memoryTableDDL4 = "create table "+memoryTable4+"(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=4);";
        String ssdTableDDL4 = "create table "+ssdTable4+"(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=4,storage_mode=\"SSD\");";
        String hddTableDDL4 = "create table "+hddTable4+"(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=4,storage_mode=\"HDD\");";
        sdkClient.execute(Lists.newArrayList(memoryTableDDL4,ssdTableDDL4,hddTableDDL4));
        // 插入一定量的数据
        sdkClient.insertList(memoryTable4,dataList);
        sdkClient.insertList(ssdTable4,dataList);
        sdkClient.insertList(hddTable4,dataList);
        Tool.sleep(5*1000);
        addDataCheck(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable4,ssdTable4,hddTable4),dataCount,0);
        // 创建表制定分片到新的tablet上，可以成功。
        String memoryTable5 = "test_memory5";
        String ssdTable5 = "test_ssd5";
        String hddTable5 = "test_hdd5";
        String memoryTableDDL5 = "create table "+memoryTable5+"(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=1,replicanum=1,distribution = [ ('"+addTabletEndpoint+"',[])]);";
        String ssdTableDDL5 = "create table "+ssdTable5+"(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=1,replicanum=1,storage_mode=\"SSD\",distribution = [ ('"+addTabletEndpoint+"',[])]);";
        String hddTableDDL5 = "create table "+hddTable5+"(\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=1,replicanum=1,storage_mode=\"HDD\",distribution = [ ('"+addTabletEndpoint+"',[])]);";
        OpenMLDBResult memory5Result = sdkClient.execute(memoryTableDDL5);
        String addTabletMsg = "create table to new tablet failed.";
        Assert.assertTrue(memory5Result.isOk(),addTabletMsg);
        Assert.assertTrue(sdkClient.tableIsExist(memoryTable5),addTabletMsg);
        OpenMLDBResult ssd5Result = sdkClient.execute(ssdTableDDL5);
        Assert.assertTrue(ssd5Result.isOk(),addTabletMsg);
        Assert.assertTrue(sdkClient.tableIsExist(ssdTable5),addTabletMsg);
        OpenMLDBResult hdd5Result = sdkClient.execute(hddTableDDL5);
        Assert.assertTrue(hdd5Result.isOk(),addTabletMsg);
        Assert.assertTrue(sdkClient.tableIsExist(hddTable5),addTabletMsg);
        // 副本迁移，迁移后，原来的数据删除，新的tablet上增加数据。
        nsClient.confset("auto_failover","false");
        nsClient.migrate(dbName,memoryTable,addTabletEndpoint);
        nsClient.migrate(dbName,ssdTable,addTabletEndpoint);
        nsClient.migrate(dbName,hddTable,addTabletEndpoint);
        addDataCheck(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount,10);
    }

    public void addDataCheck(SDKClient sdkClient, NsClient nsClient,String dbName,List<String> tableNames,int originalCount,int addCount){
        List<List<Object>> addDataList = new ArrayList<>();
        for(int i=0;i<addCount;i++){
            String c1 = RandomStringUtils.randomAlphanumeric(8);
            List<Object> list = Lists.newArrayList(c1 + i, 1, 2, 3, 1.1, 2.1, 1590738989000L, "2020-05-01", true);
            addDataList.add(list);
        }
        String msg = "table add data check count failed.";
        for(String tableName:tableNames){
            if (CollectionUtils.isNotEmpty(addDataList)) {
                sdkClient.insertList(tableName,addDataList);
            }
            Assert.assertEquals(sdkClient.getTableRowCount(tableName),originalCount+addCount,msg);
        }
        nsClient.checkTableOffSet(dbName,null);
    }
}
