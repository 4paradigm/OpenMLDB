package com._4paradigm.fesql.spark.tools;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Thread)
public class BenchmarkWindowSample {

    static private BenchmarkWindowSampleImpl bm = new BenchmarkWindowSampleImpl();

    @Setup
    public void initExecutor() {
        bm.parseArgs(bm.loadArgs());
        bm.initExecutor();
    }

    @Benchmark
    public void runWindowSample() {
        bm.runWindowSample();
    }

    static public void main(String[] args) {
        bm.run(BenchmarkWindowSample.class, args);
    }
}
