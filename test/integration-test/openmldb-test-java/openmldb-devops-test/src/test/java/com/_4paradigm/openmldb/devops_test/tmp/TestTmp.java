package com._4paradigm.openmldb.devops_test.tmp;

import com._4paradigm.openmldb.devops_test.common.ClusterTest;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.openmldb.*;
import com._4paradigm.qa.openmldb_deploy.util.Tool;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TestTmp extends ClusterTest {
    private String dbName;
    private SDKClient sdkClient;
    private NsClient nsClient;
    private OpenMLDBDevops openMLDBDevops;
    @BeforeClass
    public void beforeClass(){
        dbName = "test_devops2";
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
//        openMLDBDevops.operateTablet(0,"stop");
//        String oneTabletStopMsg = "tablet1 stop table row count check failed.";
//        Assert.assertEquals(sdkClient.getTableRowCount(memoryTable),dataCount,oneTabletStopMsg);
//        Assert.assertEquals(sdkClient.getTableRowCount(ssdTable),dataCount,oneTabletStopMsg);
//        Assert.assertEquals(sdkClient.getTableRowCount(hddTable),dataCount,oneTabletStopMsg);
//        // tablet start，数据可以回复，要看磁盘表和内存表。
//        openMLDBDevops.operateTablet(0,"start");
//        addDataCheck(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount,10);
//        //创建磁盘表和内存表，在重启tablet，数据可回复，内存表和磁盘表可以正常访问。
//        openMLDBDevops.operateTablet(0,"restart");
//        addDataCheck(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+10,10);
//        //创建磁盘表和内存表，插入一些数据，然后make snapshot，在重启tablet，数据可回复。
//        nsClient.makeSnapshot(dbName,memoryTable);
//        nsClient.makeSnapshot(dbName,ssdTable);
//        nsClient.makeSnapshot(dbName,hddTable);
//        //tablet 依次restart，数据可回复，可以访问。
//        openMLDBDevops.operateTablet("restart");
//        addDataCheck(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+20,10);
        //1个ns stop，可以正常访问。
        openMLDBDevops.operateNs(0,"stop");
        resetClient();
        addDataCheck(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount,0);
        // 1个ns start 可以访问。
        openMLDBDevops.operateNs(0,"start");
        addDataCheck(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount,0);
        // 1个ns restart 可以访问。
//        openMLDBDevops.operateNs(0,"restart");
//        resetClient();
//        addDataCheck(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount,0);
//        // 单zk stop 在start后 可以访问
//        openMLDBDevops.operateZKOne("stop");
//        Tool.sleep(3000);
//        openMLDBDevops.operateZKOne("start");
//        Tool.sleep(3000);
//        addDataCheck(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount,0);
//        // 单zk restart 后可以访问
//        openMLDBDevops.operateZKOne("restart");
//        addDataCheck(sdkClient,nsClient,dbName,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount,0);
//        //3个tablet stop，不能访问。
//        openMLDBDevops.operateTablet("stop");
//        OpenMLDBResult openMLDBResult = sdkClient.execute(String.format("select * from %s",memoryTable));
//        Assert.assertTrue(openMLDBResult.getMsg().contains("fail"));

//        // 1个tablet启动，数据可回复，分片所在的表，可以访问。
//        openMLDBDevops.operateTablet(0,"start");
//        addDataCheck(sdkClient,nsClient,Lists.newArrayList(memoryTable,ssdTable,hddTable),dataCount+30,0);

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
