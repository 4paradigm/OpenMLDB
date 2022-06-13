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
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Threads(2)
@Fork(value = 1, jvmArgs = {"-Xms32G", "-Xmx32G"})
@Warmup(iterations = 1)
public class FESQLGroupByBenchmark {
    private ArrayList<String> dataset = new ArrayList<>();
    private SqlExecutor executor;
    private SdkOption option;
    private String db = "db" + System.nanoTime();
    private String ddl = "";
    private String query = "";
    private boolean setupOk = false;
    private int recordSize = 20000;
    private ArrayList<String> querySet = new ArrayList<>();
    private String format = "insert into %s values('%s', %d,";
    private Random random = new Random(System.currentTimeMillis());
    private long counter = 0;
    public FESQLGroupByBenchmark() {
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
    public void setup() {
        String header = "create table perf (col1 string, col2 bigint, ";
        for (int i = 0; i < 50; i++) {
            header += "col_agg" + i + " double,";
        }
        header += "index(key=col1, ts=col2));" ;
        ddl = header;
        query = "select ";
        for (int i = 0; i < 50; i++) {
            if (i == 49) {
                query += "avg(col_agg" + i + ")";
            }else {
                query += "avg(col_agg" + i + "),";
            }
        }
        query += " from perf where col1='pkxxx0'";
        for (int i = 0; i < 50; i++) {
            if (i == 49) {
                format += "2.0";
            }else {
                format += "2.0,";
            }
        }
        format+=");";
        setupOk = executor.createDB(db);
        if (!setupOk) {
            return;
        }
        setupOk = executor.executeDDL(db, ddl);
        if (!setupOk) {
            return;
        }
        for (int i = 0; i < recordSize; i++) {
            String pk = "pkxxx" + i;
            for (int j = 0; j < 1000; j++) {
                String sql = String.format(format, "perf", pk, System.currentTimeMillis());
                executor.executeInsert(db, sql);
            }
            System.out.println(i * 1000);
            querySet.add(pk);
        }
    }

    @Benchmark
    public void groupByBm() {
        long index = random.nextInt(querySet.size());
        PreparedStatement pst = null;
        try {
            pst = executor.getRequestPreparedStmt(db, query);
            if (index < 0) index = index * -1;
            String key = querySet.get((int) index);
            pst.setString(1, key);
            pst.setLong(2, System.currentTimeMillis());
            for (int i = 3; i < 50-3; i++) {

            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
    public static void main(String[] args) throws RunnerException {
       Options opt = new OptionsBuilder()
                .include(FESQLGroupByBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
