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

package com._4paradigm.sql.jmh.openmldb;

import com._4paradigm.sql.jmh.ParameterizedQueryBenchmark;
import com._4paradigm.sql.jmh.memsql.MemsqlParameterizedQueryBenchmark;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 1)
@Slf4j
public class OpenMLDBParameterizedQueryBenchmark extends OpenMLDBSetup implements ParameterizedQueryBenchmark {
    @Setup(Level.Trial)
    @Override
    public void setup() throws SQLException {
        super.setup();

        log.debug("creating table: {}", getDDL());
        if (!executor.executeDDL(getDb(), getDDL())) {
            throw new SQLException("failed to create new table");
        }
        int cnt = 0;
        try {
            for (int i = 0; i < getRecordSize() / 1000; i++) {
                for (int j = 0; j < 1000; j++) {
                    String sql = String.format(getInsertStmt(), String.format("pk-%d-%d", i, j), System.currentTimeMillis());
                    if (!executor.executeInsert(getDb(), sql)) {
                        throw new SQLException(String.format("fail to insert %dth round/%dth record", i, j));
                    } else {
                        cnt ++;
                    }
                }
            }
        } finally {
            log.info("inserting {} records of {} total", cnt, getRecordSize());
        }
    }

    @TearDown(Level.Trial)
    @Override
    public void teardown() throws SQLException {
        if (!executor.executeDDL(getDb(), getCleanDDL())) {
            throw new SQLException("teardown: failed to drop table");
        }
        super.teardown();
    }

    @Benchmark
    @Override
    public ResultSet query() throws SQLException {
        try (PreparedStatement stmt = executor.getPreparedStatement(getDb(), getQuery())) {
            stmt.setString(1, param1);
            return stmt.executeQuery();
        }
    }

    @Override
    public String getDDL() {
        return "create table " + getTableName() + " (col1 string, col2 timestamp, " +
                "col3 float," +
                "col4 double," +
                "col5 string," +
                "index(key=col1, ts=col2));";
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(OpenMLDBParameterizedQueryBenchmark.class.getSimpleName())
                .shouldFailOnError(true)
                .resultFormat(ResultFormatType.JSON)
                .result(MemsqlParameterizedQueryBenchmark.class.getSimpleName() + ".json")
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
