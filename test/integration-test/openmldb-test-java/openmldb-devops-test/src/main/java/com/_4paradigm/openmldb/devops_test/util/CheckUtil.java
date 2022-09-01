package com._4paradigm.openmldb.devops_test.util;

import com._4paradigm.openmldb.test_common.openmldb.NsClient;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import com._4paradigm.qa.openmldb_deploy.util.Tool;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.collections.Lists;

import java.util.ArrayList;
import java.util.List;

public class CheckUtil {
    public static void addDataCheckByOffset(SDKClient sdkClient, NsClient nsClient, String dbName, List<String> tableNames, int originalCount, int addCount){
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
                Tool.sleep(10*1000);
            }
            Assert.assertEquals(sdkClient.getTableRowCount(tableName),originalCount+addCount,msg);
            Assert.assertEquals(nsClient.getTableCount(dbName,tableName),originalCount+addCount,msg);
        }
        nsClient.checkTableOffSet(dbName,null);
    }
    public static void addDataCheckByCount(SDKClient sdkClient, List<String> tableNames, int originalCount, int addCount){
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
    }
}
