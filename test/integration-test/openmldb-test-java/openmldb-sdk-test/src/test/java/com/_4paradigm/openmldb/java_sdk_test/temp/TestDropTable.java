package com._4paradigm.openmldb.java_sdk_test.temp;


import com._4paradigm.openmldb.java_sdk_test.common.FedbClient;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.java_sdk_test.util.FesqlUtil;
import org.testng.annotations.Test;

import java.util.List;

public class TestDropTable {

    @Test
    public void testAll(){
        FedbClient fedbClient = new FedbClient("172.24.4.55:10000","/fedb");
        String dbName = "test_fedb";
        String sql = "show tables;";
        FesqlResult fesqlResult = FesqlUtil.select(fedbClient.getExecutor(), dbName, sql);
        List<List<Object>> result = fesqlResult.getResult();
        for(List<Object> list:result){
            System.out.println(list);
        }
    }
}
