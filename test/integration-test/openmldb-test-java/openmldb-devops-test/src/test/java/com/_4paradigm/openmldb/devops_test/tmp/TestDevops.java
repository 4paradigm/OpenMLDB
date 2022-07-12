package com._4paradigm.openmldb.devops_test.tmp;

import com._4paradigm.openmldb.devops_test.common.ClusterTest;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.openmldb.NsClient;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBDevops;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.util.ArrayList;
import java.util.List;

public class TestDevops extends ClusterTest {
    private String dbName;
    private String memoryTable;
    private String ssdTable;
    private String hddTable;
    private SDKClient sdkClient;
    private NsClient nsClient;
    private OpenMLDBDevops openMLDBDevops;
    @BeforeClass
    public void beforeClass(){
        dbName = "test_devops2";
        memoryTable = "test_memory";
        ssdTable = "test_ssd";
        hddTable = "test_hdd";
        sdkClient = SDKClient.of(executor);
        nsClient = NsClient.of(OpenMLDBGlobalVar.mainInfo);
        openMLDBDevops = OpenMLDBDevops.of(OpenMLDBGlobalVar.mainInfo,dbName);
    }
    @Test
    public void test1(){
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
    }
    @Test
    public void test2(){
        sdkClient.createAndUseDB(dbName);
        OpenMLDBResult openMLDBResult = sdkClient.execute(String.format("select * from %s",memoryTable));
        System.out.println(openMLDBResult.getMsg());
//        List<String> lines = nsClient.runNs(dbName, "showtable");
//        System.out.println("lines = " + lines);
    }
    @Test
    public void test3(){
        openMLDBDevops.operateNs(0,"stop");
    }
}
