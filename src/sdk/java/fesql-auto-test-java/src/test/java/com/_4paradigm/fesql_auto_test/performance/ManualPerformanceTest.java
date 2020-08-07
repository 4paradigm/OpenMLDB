package com._4paradigm.fesql_auto_test.performance;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class ManualPerformanceTest {
    @Test(enabled = false)
    public void requestAggExample8ThreadTest() {
        RequestAggExample.run(8);
    }
}
