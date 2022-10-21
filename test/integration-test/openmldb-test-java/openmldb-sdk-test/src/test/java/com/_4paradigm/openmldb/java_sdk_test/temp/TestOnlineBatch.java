package com._4paradigm.openmldb.java_sdk_test.temp;

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBTest;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import com._4paradigm.openmldb.test_common.util.SDKUtil;
import org.testng.annotations.Test;

public class TestOnlineBatch extends OpenMLDBTest {
    @Test
    public void test(){
        SDKClient sdkClient = SDKClient.of(executor);
        sdkClient.createAndUseDB("test_batch");
        sdkClient.setOnline();
        sdkClient.execute("create table t1(\n" +
                "id int,\n" +
                "c1 string,\n" +
                "c3 int,\n" +
                "c4 bigint,\n" +
                "c5 float,\n" +
                "c6 double,\n" +
                "c7 timestamp,\n" +
                "c8 date,\n" +
                "index(key=(c1),ts=c7))options(partitionnum=2,replicanum=1);");
        for (int i=1;i<=100000;i++){
            int tmp = i%10;
            sdkClient.execute(String.format("insert into %s values(1,'aa%d',%d,30,1.1,2.1,%d,'2020-05-01');","t1",tmp,i,i));
        }
    }
}
