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
import com._4paradigm.openmldb.jmh.performance.FESQLFZBenchmark;
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
import java.util.List;
import java.util.Map;

public class FESQLFZBenchmarkTest {
    private static Logger logger = LoggerFactory.getLogger(FESQLClusterBenchmark.class);

    @Test
    public void execSQLTest() throws SQLException {
        FESQLFZBenchmark benchmark = new FESQLFZBenchmark(true, false);
        benchmark.setup();
        int loops = 1;
        for (int i = 0; i < loops; i++) {
            List<Map<String, String>> results = benchmark.execSQLTest();
            Assert.assertTrue(results.size() > 0);
            System.out.println("result sizes " + results.size());
            for (int idx = 0; idx < results.size(); idx++) {
                Map<String, String> result = results.get(idx);
                System.out.println("=========================================result row " + idx);
                for (Map.Entry<String, String> entry : result.entrySet()) {
                    System.out.println(entry.getKey() + ": " + entry.getValue());
                }
                Assert.assertNotNull(result);
                Assert.assertTrue(result.size() > 0);
            }
        }
        benchmark.teardown();
    }

    @Test
    public void dumpSQLCaseTest() throws SQLException {
        try {
            FESQLFZBenchmark benchmark = new FESQLFZBenchmark(true, true);
            benchmark.setWindowNum(5);
            benchmark.setup();
            int loops = 1;
            for (int i = 0; i < loops; i++) {
                List<Map<String, String>> results = benchmark.execSQLTest();
                System.out.println("result sizes " + results.size());
                Assert.assertTrue(results.size() > 0);
                for (int idx = 0; idx < results.size(); idx++) {
                    Map<String, String> result = results.get(idx);
                    System.out.println("=========================================result row " + idx);
                    for (Map.Entry<String, String> entry : result.entrySet()) {
                        System.out.println(entry.getKey() + ": " + entry.getValue());
                    }
                    Assert.assertNotNull(result);
                    Assert.assertTrue(result.size() > 0);
                }
            }
            benchmark.outputSQLCase("fz_case_benchmark.yaml");
            benchmark.teardown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Ignore
    public void benchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FESQLFZBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Test
    @Ignore
    public void benchmarkSampleTime() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FESQLFZBenchmark.class.getSimpleName())
                .mode(Mode.SampleTime)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Test
    @Ignore
    public void benchmarkAverageTime() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FESQLFZBenchmark.class.getSimpleName())
                .mode(Mode.AverageTime)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
