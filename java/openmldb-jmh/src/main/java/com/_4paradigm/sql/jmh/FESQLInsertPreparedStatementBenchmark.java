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

package com._4paradigm.sql.jmh;

import com._4paradigm.sql.BenchmarkConfig;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Threads(10)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 1)
public class FESQLInsertPreparedStatementBenchmark {
    private SqlExecutor executor;
    private SdkOption option;
    private String db = "db" + System.nanoTime();
    private String ddl = "create table perf (col1 string, col2 bigint, " +
            "col3 float," +
            "col4 double," +
            "col5 string," +
            "index(key=col1, ts=col2));";
    private String ddl1 = "create table perf2 (col1 string, col2 bigint, " +
            "col3 float," +
            "col4 double," +
            "col5 string," +
            "index(key=col1, ts=col2));";
    private boolean setupOk = false;
    private int recordSize = 10000;
    private String format = "insert into perf values(?, ?, 100.0, 200.0, 'hello world');";
    private String format2 = "insert into %s values('%s', %d, 100.0, 200.0, 'hello world');";
    private long counter = 0;

    public FESQLInsertPreparedStatementBenchmark() {
        SdkOption sdkOption = new SdkOption();
        sdkOption.setZkSessionTimeout(30000);
        sdkOption.setZkCluster(BenchmarkConfig.ZK_CLUSTER);
        sdkOption.setZkPath(BenchmarkConfig.ZK_PATH);
        this.option = sdkOption;
        try {
            executor = new SqlClusterExecutor(option);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Setup
    public void setup() throws SQLException {
        setupOk = executor.createDB(db);
        if (!setupOk) {
            return;
        }
        setupOk = executor.executeDDL(db, ddl);
        if (!setupOk) {
            return;
        }
        setupOk = executor.executeDDL(db, ddl1);
        if (!setupOk) {
            return;
        }
        for (int i = 0; i < recordSize / 100; i++) {
            String sql = String.format(format2, "perf2", "pkxxx" + i, System.currentTimeMillis());
            executor.executeInsert(db, sql);
        }
    }

    @Benchmark
    public void insertBm() {
        /*
        long idx = counter;
        PreparedStatement impl = executor.getInsertPrepareStmt(db, format);
        try {
            impl.setString(1, "pkxxx" + counter);
            impl.setLong(2, System.currentTimeMillis());
            impl.execute();
        } catch (Exception e) {

        }
        counter ++;
         */
        long idx = counter;
        PreparedStatement impl = null;
        try {
            impl = executor.getInsertPreparedStmt(db, format);
            for (int i = 0; i < 10; i++) {
                String s1 = "pkxxx" + idx + i;
                impl.setString(1, s1);
                impl.setLong(2, System.currentTimeMillis());
                impl.addBatch();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            impl.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
        }
        counter += 10;
    }

    @Benchmark
    public void selectSimpleBm() {
        String sql = "select col1, col2, col3 from perf2 limit 10;";
        java.sql.ResultSet rs = executor.executeSQL(db, sql);
    }

    @Benchmark
    public void select150Feature() {
        String sql = "select col1, col2, col3";
        for (int i = 0; i < 50; i++) {
            sql += String.format(", col1 as col1%d, col2 as col2%d, col3 as col3%d", i, i, i);
        }
        sql += " from perf2 limit 1;";
        java.sql.ResultSet rs = executor.executeSQL(db, sql);
    }

    @Benchmark
    public void select510Feature() {
        String sql = "select col1, col2, col3";
        for (int i = 0; i < 170; i++) {
            sql += String.format(", col1 as col1%d, col2 as col2%d, col3 as col3%d", i, i, i);
        }
        sql += " from perf2 limit 1;";
        java.sql.ResultSet rs = executor.executeSQL(db, sql);
    }

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(FESQLInsertPreparedStatementBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
