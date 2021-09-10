/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com._4paradigm.openmldb.jmh.test;


import com._4paradigm.openmldb.jmh.performance.FESQLClusterBenchmark;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

public class FESQLClusterBenchmarkTest {
    private static Logger logger = LoggerFactory.getLogger(FESQLClusterBenchmark.class);
    @Test
    public void execSQLTest() throws SQLException {
        FESQLClusterBenchmark benchmark = new FESQLClusterBenchmark(true);
        benchmark.setWindowNum(1000);
        benchmark.setup();
        int loops = 1;
        for (int i = 0; i < loops; i++) {
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
    }
    @Test
    @Ignore
    public void benchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FESQLClusterBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
    @Test
    @Ignore
    public void benchmarkSampleTime() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FESQLClusterBenchmark.class.getSimpleName())
                .mode(Mode.SampleTime)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
    @Test
    @Ignore
    public void benchmarkAverageTime() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FESQLClusterBenchmark.class.getSimpleName())
                .mode(Mode.AverageTime)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
