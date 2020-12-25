package com._4paradigm.sql.jmh;

import com._4paradigm.sql.tools.Util;
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

public class FESQLFZBenchmarkTest {
    private static Logger logger = LoggerFactory.getLogger(FESQLClusterBenchmark.class);
    @Test
    public void execSQLTest() throws SQLException {
        FESQLFZBenchmark benchmark = new FESQLFZBenchmark(false);
        Util.EnableProxy();
        benchmark.setWindowNum(10);
        benchmark.setup();
        int loops = 1;
        for (int i = 0; i < loops; i++) {
            Map<String, String> result = benchmark.execSQLTest();
            for(Map.Entry<String, String> entry: result.entrySet()) {
                System.out.println(entry.getKey()  + ": " + entry.getValue());
            }
            Assert.assertNotNull(result);
            Assert.assertTrue(result.size() > 0);
            System.out.println("reqId_1: " + result.get("reqId_1"));
            System.out.println("reqId_243: " + result.get("reqId_243"));
            System.out.println("----------------------------");
        }
        benchmark.teardown();
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
