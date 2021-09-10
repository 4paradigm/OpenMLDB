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
package com._4paradigm.openmldb.jmh.performance;


import com._4paradigm.openmldb.jmh.BenchmarkConfig;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 2)
public class MemSQLBenchmark {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private ArrayList<String> dataset = new ArrayList<>();
    private int recordSize = 100000;
    private Connection cnn;
    private String format = "insert into %s values('%s', %d," +
            "100.0, 200.0, 'hello world');";
    private String ddl = "create table perf (col1 varchar(20), col2 bigint, " +
            "col3 float," +
            "col4 double," +
            "col5 varchar(2)," +
            "KEY (col1, col2));";
    private String ddl1 = "create table perf2 (col1 varchar(20), col2 bigint, " +
            "col3 float," +
            "col4 double," +
            "col5 varchar(2)," +
            "KEY (col1, col2));";
    private String ddl0 = "drop table perf;";
    private long counter = 0;
    public MemSQLBenchmark() {
    }
    @Setup
    public void setup() {
        try {

            cnn = DriverManager.getConnection(BenchmarkConfig.MEMSQL_URL);
            Statement st = cnn.createStatement();
            try {
                st.execute(ddl0);
                st.execute("drop table perf2;");
            } catch (Exception e) {
            }
            st = cnn.createStatement();
            st.execute(ddl);
            st.execute(ddl1);
            for (int i = 0; i < recordSize/100; i++) {
                for (int j = 0; j < 100; j++) {
                    dataset.add(String.format(format, "perf", "pkxxx" + i, System.currentTimeMillis()));
                }
                Statement st0 = cnn.createStatement();
                st0.execute(String.format(format, "perf2", "pkxxx" + i, System.currentTimeMillis()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    public void insertBm() {
        long idx = counter % dataset.size();
        String sql = dataset.get((int)idx);
        counter ++;
        try {
            Statement st = cnn.createStatement();
            st.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    public void selectSimpleBm() {
        String sql = "select col1, col2, col3 from perf2 limit 10;";
        try {
            Statement st = cnn.createStatement();
            st.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Benchmark
    public void select150Feature() {
        String sql = "select col1, col2, col3";
        for (int i = 0; i < 50; i++) {
            sql += String.format(", col1 as col1%d, col2 as col2%d, col3 as col3%d", i, i, i);
        }
        sql += " from perf2 limit 1;";
        try {
            Statement st = cnn.createStatement();
            st.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    public void select510Feature() {
        String sql = "select col1, col2, col3";
        for (int i = 0; i < 170; i++) {
            sql += String.format(", col1 as col1%d, col2 as col2%d, col3 as col3%d", i, i, i);
        }
        sql += " from perf2 limit 1;";
        try {
            Statement st = cnn.createStatement();
            st.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MemSQLBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
