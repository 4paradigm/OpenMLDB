package com._4paradigm.sql.jmh.memsql;

import com._4paradigm.sql.jmh.ParameterizedQuery2Benchmark;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 1)
@Slf4j
public class MemsqlParameterizedQuery2Benchmark extends MemsqlSetup implements ParameterizedQuery2Benchmark {
    @Override
    @Setup(Level.Trial)
    public void setup() throws SQLException {
        super.setup();
        prepareData();
    }

    @Override
    @TearDown(Level.Trial)
    public void teardown() throws SQLException {
        cleanup();
        super.teardown();
    }

    @Benchmark
    public ResultSet bm() throws SQLException {
        return query();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MemsqlParameterizedQuery2Benchmark.class.getSimpleName())
                .shouldFailOnError(true)
                .resultFormat(ResultFormatType.JSON)
                .result(MemsqlParameterizedQueryBenchmark.class.getSimpleName() + ".json")
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
