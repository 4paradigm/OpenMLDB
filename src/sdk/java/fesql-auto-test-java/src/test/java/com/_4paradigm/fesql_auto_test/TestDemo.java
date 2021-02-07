package com._4paradigm.fesql_auto_test;

import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.sql.ResultSet;
import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.SqlException;
import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import org.testng.annotations.Test;

/**
 * @author zhaowei
 * @date 2020/6/8 下午4:59
 */
public class TestDemo {
    @Test
    public void demo() throws SqlException {
        SdkOption option = new SdkOption();
        option.setZkPath(FesqlConfig.ZK_ROOT_PATH);
        option.setZkCluster(FesqlConfig.ZK_CLUSTER);
        option.setSessionTimeout(200000);
        SqlExecutor router = new SqlClusterExecutor(option);
        System.out.println(">>:" + router);
        String dbname = "test_zw";
//        router.createDB(dbname);
        String createTable = "create table t1(col1 string,col2 timestamp,col3 double,index(key=col1,ts=col2));";
        boolean createOk = router.executeDDL(dbname, createTable);

        System.out.println("create:" + createOk);
        String insert = "insert into t1 values('hello',1590738989000L, 10.0);";
        boolean ok = router.executeInsert(dbname, insert);
        System.out.println("insert:" + ok);
        String select = "select * from t1;";
        java.sql.ResultSet rawRs = router.executeSQL(dbname, select);
        if (rawRs instanceof ResultSet) {
            ResultSet rs = (ResultSet) rawRs;
            System.out.println(rs.Size());
            while (rs.Next()) {
                System.out.println(">>1:" + rs.GetAsString(0));
                System.out.println(">>2:" + rs.GetTimeUnsafe(1));
                System.out.println(">>3:" + rs.GetDoubleUnsafe(2));
            }
        }
    }

    @Test
    public void test2() {
        System.out.println("111");
    }

}
