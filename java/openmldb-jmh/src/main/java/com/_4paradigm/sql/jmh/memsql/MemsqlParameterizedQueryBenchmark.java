package com._4paradigm.sql.jmh.memsql;

import com._4paradigm.sql.jmh.ParameterizedQueryBenchmark;
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
public class MemsqlParameterizedQueryBenchmark extends MemsqlSetup implements ParameterizedQueryBenchmark {
    @Override
    @Setup(Level.Trial)
    public void setup() throws SQLException {
        super.setup();
        // prepare the data
        try (Statement createStmt = connection.createStatement()) {
            createStmt.execute(getDDL());
        }

        int cnt = 0;
        try (Statement insertStmt = connection.createStatement()) {
            for (int i = 0; i < getRecordSize() / 1000; i++) {
                for (int j = 0; j < 1000; j++) {
                    String sql = String.format(getInsertStmt(), String.format("pk-%d-%d", i, j), System.currentTimeMillis());
                    insertStmt.execute(sql);
                    cnt ++;
                }
            }
        } finally {
            log.info("inserted {}/{} records", cnt, getRecordSize());
        }
    }

    @Override
    @TearDown(Level.Trial)
    public void teardown() throws SQLException {
        if (connection != null) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(getCleanDDL());
            }
        }
        super.teardown();
    }

    @Override
    @Benchmark
    public ResultSet query() throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(getQuery())) {
            stmt.setString(1, param1);
            return stmt.executeQuery();
        }
    }

    @Override
    public String getDDL() {
        return String.format("create table %s (col1 varchar(128), col2 timestamp, " +
                "col3 float," +
                "col4 float," +
                "col5 varchar(128)," +
                "primary key (col1));", getTableName());
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MemsqlParameterizedQueryBenchmark.class.getSimpleName())
                .shouldFailOnError(true)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
