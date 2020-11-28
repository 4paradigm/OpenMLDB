package com._4paradigm.sql.jmh;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Map;

public class FESQLClusterBenchmarkTest {
    private static Logger logger = LoggerFactory.getLogger(FESQLClusterBenchmark.class);
    @Test
    public void windowLastJoinTest() throws SQLException {
        FESQLClusterBenchmark benchmark = new FESQLClusterBenchmark();
        benchmark.setWindowNum(1000);
        benchmark.setup();
        for(int i = 0; i< 10; i++) {
            Map<String, String> result = benchmark.execSQLTest();
            Assert.assertNotNull(result);
            Assert.assertTrue(result.size() > 0);
            System.out.println("previous_application_SK_ID_CURR_time_0s_32d_CNT: " + result.get("previous_application_SK_ID_CURR_time_0s_32d_CNT"));
            System.out.println("POS_CASH_balance_SK_ID_CURR_time_0s_32d_CNT: " + result.get("POS_CASH_balance_SK_ID_CURR_time_0s_32d_CNT"));
            System.out.println("installments_payments_SK_ID_CURR_time_0s_32d_CNT: " + result.get("installments_payments_SK_ID_CURR_time_0s_32d_CNT"));
            System.out.println("bureau_balance_SK_ID_CURR_time_0s_32d_CNT: " + result.get("bureau_balance_SK_ID_CURR_time_0s_32d_CNT"));
            System.out.println("credit_card_balance_SK_ID_CURR_time_0s_32d_CNT: " + result.get("credit_card_balance_SK_ID_CURR_time_0s_32d_CNT"));
            System.out.println("bureau_SK_ID_CURR_time_0s_32d_CNT: " + result.get("bureau_SK_ID_CURR_time_0s_32d_CNT"));
            System.out.println("----------------------------");
        }
        benchmark.teardown();
//        for(Map.Entry entry: result.entrySet()) {
//            System.out.println(entry.getKey() + ": " + entry.getValue());
//        }
    }
}
