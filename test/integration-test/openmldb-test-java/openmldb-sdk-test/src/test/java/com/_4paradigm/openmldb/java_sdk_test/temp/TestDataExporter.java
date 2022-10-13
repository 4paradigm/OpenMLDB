package com._4paradigm.openmldb.java_sdk_test.temp;

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBTest;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import org.testng.annotations.Test;

public class TestDataExporter extends OpenMLDBTest {
    @Test
    public void testSmoke(){
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute("create table test_smoke(\n" +
                "id int,\n" +
                "ts timestamp,\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=ts))options(partitionnum=1,replicanum=1);");
        sdkClient.execute("insert into test_smoke values (1,1590738989000,'aa',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);");
        sdkClient.execute("insert into test_smoke values (2,1590738990000,'bb',1,21,31,1.1,2.1,1590738989000,'2020-05-01',false);");
        sdkClient.execute("insert into test_smoke values (3,1590738991000,'cc',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);");
    }
    @Test
    public void testData1(){
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute("create table test_data1(\n" +
                "id int,\n" +
                "ts timestamp,\n" +
                "c1 string,\n" +
                "c2 smallint,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "c9 bool,\n" +
                "index(key=(c1),ts=ts))options(partitionnum=8,replicanum=3);");
        for(int i=0;i<10;i++) {
            sdkClient.execute("insert into test_data1 values (1,1590738989000,'aa"+i+"',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);");
            sdkClient.execute("insert into test_data1 values (2,1590738990000,'bb"+i+"',1,21,31,1.1,2.1,1590738989000,'2020-05-01',false);");
            sdkClient.execute("insert into test_data1 values (3,1590738991000,'cc"+i+"',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);");
        }
    }
}
