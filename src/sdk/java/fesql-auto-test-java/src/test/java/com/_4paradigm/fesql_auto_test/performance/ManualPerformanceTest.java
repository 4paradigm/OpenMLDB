package com._4paradigm.fesql_auto_test.performance;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class ManualPerformanceTest {
    @Test(enabled = false)
    public void requestAggExample8ThreadTest() {

        RequestAggExample.run(RequestAggExample.ExecuteType.kRequestAgg, 8);
    }

    @Test(enabled = false)
    public void requestInsertExample8ThreadTest() {
        RequestAggExample.run(RequestAggExample.ExecuteType.kRequestInsert, 8);
    }
}
