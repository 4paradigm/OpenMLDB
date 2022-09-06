package com._4paradigm.openmldb.devops_test.high_availability;

import com._4paradigm.openmldb.devops_test.common.ClusterTest;
import com._4paradigm.openmldb.devops_test.util.CheckUtil;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.openmldb.*;
import com._4paradigm.qa.openmldb_deploy.util.Tool;
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
        dbName = "test_devops";
        sdkClient = SDKClient.of(executor);
        nsClient = NsClient.of(OpenMLDBGlobalVar.mainInfo);
        openMLDBDevops = OpenMLDBDevops.of(OpenMLDBGlobalVar.mainInfo,dbName);
    }
    @Test
    public void testMoreReplica(){
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
        // 其中一个tablet stop，leader 内存表和磁盘表可以正常访问，flower 内存表和磁盘表可以正常访问。
        openMLDBDevops.operateTablet(0,"stop");
        String oneTabletStopMsg = "tablet1 stop table row count check failed.";
        Assert.assertEquals(sdkClient.getTableRowCount(memoryTable),dataCount,oneTabletStopMsg);
        Assert.assertEquals(sdkClient.getTableRowCount(ssdTable),dataCount,oneTabletStopMsg);
        Assert.assertEquals(sdkClient.getTableRowCount(hddTable),dataCount,oneTabletStopMsg);
        // tablet start，数据可以回复，要看磁盘表和内存表。
        openMLDBDevops.operateTablet(0,"start");
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount,10);
        //创建磁盘表和内存表，在重启tablet，数据可回复，内存表和磁盘表可以正常访问。
        openMLDBDevops.operateTablet(0,"restart");
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+10,10);
        //创建磁盘表和内存表，插入一些数据，然后make snapshot，在重启tablet，数据可回复。
        nsClient.makeSnapshot(dbName,memoryTable);
        nsClient.makeSnapshot(dbName,ssdTable);
        nsClient.makeSnapshot(dbName,hddTable);
        //tablet 依次restart，数据可回复，可以访问。
        openMLDBDevops.operateTablet("restart");
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+20,10);
        //1个ns stop，可以正常访问。
        openMLDBDevops.operateNs(0,"stop");
        resetClient();
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+30,0);
        // 1个ns start 可以访问。
        openMLDBDevops.operateNs(0,"start");
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+30,0);
        // 1个ns restart 可以访问。
        openMLDBDevops.operateNs(0,"restart");
        resetClient();
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+30,0);
        // 单zk stop 在start后 可以访问
        openMLDBDevops.operateZKOne("stop");
        Tool.sleep(3000);
        openMLDBDevops.operateZKOne("start");
        Tool.sleep(3000);
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+30,0);
        // 单zk restart 后可以访问
        openMLDBDevops.operateZKOne("restart");
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+30,0);
        // 2个tablet stop 可以访问
        openMLDBDevops.operateTablet(0,"stop");
        openMLDBDevops.operateTablet(1,"stop");
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+30,0);
        //3个tablet stop，不能访问。
        openMLDBDevops.operateTablet(2,"stop");
        OpenMLDBResult openMLDBResult = sdkClient.execute(String.format("select * from %s",memoryTable));
        Assert.assertTrue(openMLDBResult.getMsg().contains("fail"));

//        // 1个tablet启动，数据可回复，分片所在的表，可以访问。
//        openMLDBDevops.operateTablet(0,"start");
//        CheckUtil.addDataCheck(sdkClient,nsClient,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+30,0);

        //2个ns stop，不能访问。
//        openMLDBDevops.operateNs(1,"stop");
//        List<String> lines = nsClient.runNs(dbName, "showtable");
//        System.out.println(openMLDBResult.getMsg());

        //一个 zk stop，可以正常访问
        //3个zk stop，不能正常访问。
        //一个zk start，可正常访问。
        //3个 zk start，可正常访问。
        // 一个节点（ns leader 所在服务器）重启，leader可以正常访问，flower可以正常访问。
        //一直查询某一个表，然后重启一个机器。
    }
    // 两个Tablet停止
    // 三个Tablet停止

    @Test
    public void testSingle(){
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
        // tablet stop，不能访问
        openMLDBDevops.operateTablet(0,"stop");
        OpenMLDBResult openMLDBResult = sdkClient.execute(String.format("select * from %s",memoryTable));
        Assert.assertTrue(openMLDBResult.getMsg().contains("fail"));
        // tablet start，数据可以回复，要看磁盘表和内存表。
        openMLDBDevops.operateTablet(0,"start");
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount,10);
        //make snapshot，在重启tablet，数据可回复。
        nsClient.makeSnapshot(dbName,memoryTable);
        nsClient.makeSnapshot(dbName,ssdTable);
        nsClient.makeSnapshot(dbName,hddTable);
        //重启tablet，数据可回复，内存表和磁盘表可以正常访问。
        openMLDBDevops.operateTablet(0,"restart");
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+10,10);
        //ns stop start 可以正常访问。
        openMLDBDevops.operateNs(0,"stop");
//        resetClient();
        //ns start 可以访问。
        openMLDBDevops.operateNs(0,"start");
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+20,0);
        //ns restart 可以访问。
        openMLDBDevops.operateNs(0,"restart");
//        resetClient();
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+20,0);
        // stop tablet ns 后 在启动 ns tablet 可以访问
        openMLDBDevops.operateTablet(0,"stop");
        openMLDBDevops.operateNs(0,"stop");
//        resetClient();
        openMLDBDevops.operateNs(0,"start");
        Tool.sleep(10*1000);
        openMLDBDevops.operateTablet(0,"start");
        CheckUtil.addDataCheckByOffset(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+20,10);
    }
    public void resetClient(){
        OpenMLDBClient openMLDBClient = new OpenMLDBClient(OpenMLDBGlobalVar.mainInfo.getZk_cluster(), OpenMLDBGlobalVar.mainInfo.getZk_root_path());
        executor = openMLDBClient.getExecutor();
        sdkClient = SDKClient.of(executor);
        nsClient = NsClient.of(OpenMLDBGlobalVar.mainInfo);
        openMLDBDevops = OpenMLDBDevops.of(OpenMLDBGlobalVar.mainInfo,dbName);
        sdkClient.setOnline();
        sdkClient.createAndUseDB(dbName);
    }
}
