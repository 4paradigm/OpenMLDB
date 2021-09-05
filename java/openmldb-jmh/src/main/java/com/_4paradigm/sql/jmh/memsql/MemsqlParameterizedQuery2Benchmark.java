package com._4paradigm.sql.jmh.memsql;

import com._4paradigm.sql.jmh.ParameterizedQuery2Benchmark;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
        try (Statement createStmt = connection.createStatement()) {
            createStmt.execute(getDDL());
            // create a non-unique index for col3
            createStmt.execute(createIndexSql());
        }


        int cnt = 0;
        try (Statement insertStmt = connection.createStatement()) {
            int val;
            for (int i = 0; i < getRecordSize(); i++) {
                val = i % 100;
                String sql = String.format(getInsertStmt(), i, val, System.currentTimeMillis());
                insertStmt.execute(sql);
                cnt ++;
            }
        } finally {
            log.info("inserted {}/{} records", cnt, getRecordSize());
        }
    }

    @TearDown(Level.Trial)
    @Override
    public void teardown() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(getCleanDDL());
        }
        super.teardown();
    }

    @Benchmark
    @Override
    public ResultSet query() throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(getQuery())) {
            stmt.setInt(1, param1);
            stmt.setInt(2, param2);
            return stmt.executeQuery();
        }
    }

    private String createIndexSql() {
        return "create index col3_index on " + getTableName() + " (col2)";
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MemsqlParameterizedQuery2Benchmark.class.getSimpleName())
                .shouldFailOnError(true)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
