package com._4paradigm.openmldb.java_sdk_test.temp;

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBTest;
import com._4paradigm.openmldb.java_sdk_test.common.StandaloneTest;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import org.testng.annotations.Test;

public class TestDataExporterByStandalone extends StandaloneTest {
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
        // ./data_exporter --db_name=test_data --table_name=test_smoke --config_path=test-standalone.yaml
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
        // ./data_exporter --db_name=test_data --table_name=test_data1 --config_path=test.yaml
    }
    @Test
    public void testTwoIndex(){
        String tableName = "test_data2";
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute(String.format("create table %s(\n" +
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
                "index(key=(c1),ts=ts),index(key=(c2),ts=ts)" +
                ")options(partitionnum=8,replicanum=3);",tableName));
        sdkClient.execute(String.format("insert into %s values (1,1590738989000,'aa',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);",tableName));
        sdkClient.execute(String.format("insert into %s values (2,1590738990000,'bb',2,22,32,1.1,2.1,1590738989000,'2020-05-01',false);",tableName));
        sdkClient.execute(String.format("insert into %s values (3,1590738991000,'cc',3,23,33,1.1,2.1,1590738989000,'2020-05-01',true);",tableName));
        // ./data_exporter --db_name=test_data --table_name=test_data2 --config_path=test.yaml
    }
    @Test
    public void testUnionIndex(){
        String tableName = "test_data3";
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute(String.format("create table %s(\n" +
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
                "index(key=(c1,c2),ts=ts)" +
                ")options(partitionnum=8,replicanum=3);",tableName));
        sdkClient.execute(String.format("insert into %s values (1,1590738989000,'aa',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);",tableName));
        sdkClient.execute(String.format("insert into %s values (2,1590738990000,'bb',2,22,32,1.1,2.1,1590738989000,'2020-05-01',false);",tableName));
        sdkClient.execute(String.format("insert into %s values (3,1590738991000,'cc',3,23,33,1.1,2.1,1590738989000,'2020-05-01',true);",tableName));
        // ./data_exporter --db_name=test_data --table_name=test_data3 --config_path=test.yaml
    }
    @Test
    public void testNoTs(){
        String tableName = "test_data4";
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute(String.format("create table %s(\n" +
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
                "c9 bool);",tableName));
        sdkClient.execute(String.format("insert into %s values (1,1590738989000,'aa',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);",tableName));
        sdkClient.execute(String.format("insert into %s values (2,1590738990000,'bb',2,22,32,1.1,2.1,1590738989000,'2020-05-01',false);",tableName));
        sdkClient.execute(String.format("insert into %s values (3,1590738991000,'cc',3,23,33,1.1,2.1,1590738989000,'2020-05-01',true);",tableName));
        // ./data_exporter --db_name=test_data --table_name=test_data4 --config_path=test.yaml
    }
    @Test
    public void testEmpty(){
        String tableName = "test_data5";
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute(String.format("create table %s(\n" +
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
                "c9 bool);",tableName));
        // ./data_exporter --db_name=test_data --table_name=test_data5 --config_path=test.yaml
    }
    @Test
    public void testHDD(){
        String tableName = "test_data6";
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute(String.format("create table %s(\n" +
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
                "index(key=(c1,c2),ts=ts)" +
                ")options(partitionnum=8,replicanum=3,storage_mode='HDD');",tableName));
        sdkClient.execute(String.format("insert into %s values (1,1590738989000,'aa',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);",tableName));
        sdkClient.execute(String.format("insert into %s values (2,1590738990000,'bb',2,22,32,1.1,2.1,1590738989000,'2020-05-01',false);",tableName));
        sdkClient.execute(String.format("insert into %s values (3,1590738991000,'cc',3,23,33,1.1,2.1,1590738989000,'2020-05-01',true);",tableName));
        // ./data_exporter --db_name=test_data --table_name=test_data6 --config_path=test.yaml
    }
    @Test
    public void testSSD(){
        String tableName = "test_data7";
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute(String.format("create table %s(\n" +
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
                "index(key=(c1,c2),ts=ts)" +
                ")options(partitionnum=8,replicanum=3,storage_mode='SSD');",tableName));
        sdkClient.execute(String.format("insert into %s values (1,1590738989000,'aa',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);",tableName));
        sdkClient.execute(String.format("insert into %s values (2,1590738990000,'bb',2,22,32,1.1,2.1,1590738989000,'2020-05-01',false);",tableName));
        sdkClient.execute(String.format("insert into %s values (3,1590738991000,'cc',3,23,33,1.1,2.1,1590738989000,'2020-05-01',true);",tableName));
        // ./data_exporter --db_name=test_data --table_name=test_data7 --config_path=test.yaml
    }
    @Test
    public void testSnapshot(){
        String tableName = "test_data9";
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute(String.format("create table %s(\n" +
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
                "index(key=(c1),ts=ts)" +
                ")options(partitionnum=8,replicanum=3);",tableName));
        for(int i=0;i<100;i++) {
            sdkClient.execute(String.format("insert into %s values (1,1590738989000,'aa%d',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);", tableName,i));
            sdkClient.execute(String.format("insert into %s values (2,1590738990000,'bb%d',2,22,32,1.1,2.1,1590738989000,'2020-05-01',false);", tableName,i));
            sdkClient.execute(String.format("insert into %s values (3,1590738991000,'cc%d',3,23,33,1.1,2.1,1590738989000,'2020-05-01',true);", tableName,i));
        }
        // makesnapshot test_data9 0
        // ./data_exporter --db_name=test_data --table_name=test_data9 --config_path=test.yaml
    }
    @Test
    public void testSnapshot2(){
        String tableName = "test_data10";
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute(String.format("create table %s(\n" +
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
                "index(key=(c1),ts=ts)" +
                ")options(partitionnum=1,replicanum=1);",tableName));
        for(int i=0;i<100;i++) {
            sdkClient.execute(String.format("insert into %s values (1,1590738989000,'aa%d',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);", tableName,i));
            sdkClient.execute(String.format("insert into %s values (2,1590738990000,'bb%d',2,22,32,1.1,2.1,1590738989000,'2020-05-01',false);", tableName,i));
            sdkClient.execute(String.format("insert into %s values (3,1590738991000,'cc%d',3,23,33,1.1,2.1,1590738989000,'2020-05-01',true);", tableName,i));
        }
        // makesnapshot test_data10 0
        // ./data_exporter --db_name=test_data --table_name=test_data10 --config_path=test.yaml
    }

    @Test
    public void testSnapshotAndBinlog(){
        String tableName = "test_data11";
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute(String.format("create table %s(\n" +
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
                "index(key=(c1),ts=ts)" +
                ")options(partitionnum=8,replicanum=3);",tableName));
        for(int i=0;i<100;i++) {
            sdkClient.execute(String.format("insert into %s values (1,1590738989000,'aa%d',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);", tableName,i));
            sdkClient.execute(String.format("insert into %s values (2,1590738990000,'bb%d',2,22,32,1.1,2.1,1590738989000,'2020-05-01',false);", tableName,i));
            sdkClient.execute(String.format("insert into %s values (3,1590738991000,'cc%d',3,23,33,1.1,2.1,1590738989000,'2020-05-01',true);", tableName,i));
        }
        // makesnapshot test_data11 0
        // ./data_exporter --db_name=test_data --table_name=test_data11 --config_path=test.yaml
    }
    @Test
    public void testInsert(){
        String tableName = "test_data12";
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.useDB("test_data");
        for(int i=0;i<10000;i++) {
            sdkClient.execute(String.format("insert into %s values (1,1590738989000,'aa%d',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);", tableName,i));
            sdkClient.execute(String.format("insert into %s values (2,1590738990000,'bb%d',2,22,32,1.1,2.1,1590738989000,'2020-05-01',false);", tableName,i));
            sdkClient.execute(String.format("insert into %s values (3,1590738991000,'cc%d',3,23,33,1.1,2.1,1590738989000,'2020-05-01',true);", tableName,i));
        }
    }
    @Test
    public void testSnapshotAndBinlog2(){
        String tableName = "test_data12";
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.setOnline();
        sdkClient.createAndUseDB("test_data");
        sdkClient.execute(String.format("create table %s(\n" +
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
                "index(key=(c1),ts=ts)" +
                ")options(partitionnum=1,replicanum=1);",tableName));
        for(int i=0;i<100;i++) {
            sdkClient.execute(String.format("insert into %s values (1,1590738989000,'aa%d',1,21,31,1.1,2.1,1590738989000,'2020-05-01',true);", tableName,i));
            sdkClient.execute(String.format("insert into %s values (2,1590738990000,'bb%d',2,22,32,1.1,2.1,1590738989000,'2020-05-01',false);", tableName,i));
            sdkClient.execute(String.format("insert into %s values (3,1590738991000,'cc%d',3,23,33,1.1,2.1,1590738989000,'2020-05-01',true);", tableName,i));
        }
        // makesnapshot test_data12 0
        // ./data_exporter --db_name=test_data --table_name=test_data12 --config_path=test-standalone.yaml
    }
}
