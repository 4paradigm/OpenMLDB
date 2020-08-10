package com._4paradigm.fesql_auto_test.performance;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class ManualPerformanceTest {
    @Test
    public void requestAggExample8ThreadTest() {

        boolean enablePerformanceTest = Boolean.parseBoolean(System.getenv("ENABLE_PERFORMANCE_TEST"));
        if (enablePerformanceTest) {
            RequestAggExample.run(RequestAggExample.ExecuteType.kRequestAgg, 8);
        }
    }

    @Test
    public void requestInsertExample8ThreadTest() {
        boolean enablePerformanceTest = Boolean.parseBoolean(System.getenv("ENABLE_PERFORMANCE_TEST"));
        if (enablePerformanceTest) {
            RequestAggExample.run(RequestAggExample.ExecuteType.kRequestInsert, 8);
        }
    }
}
