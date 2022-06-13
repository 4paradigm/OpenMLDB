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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 1)
public class FESQLLastJoinBenchmark {
    private ArrayList<String> dataset = new ArrayList<>();
    private SqlExecutor executor;
    private SdkOption option;
    private String db = "db" + System.nanoTime();
    private String ddl = "create table t1 (col1 string, col2 timestamp, " +
            "col3 float," +
            "col4 double," +
            "col5 string," +
            "index(key=col1, ts=col2));";

    private String ddl1 = "create table t2 (col1 string, col2 timestamp, " +
            "col3 float," +
            "col4 double," +
            "col5 string," +
            "index(key=col1, ts=col2));";

    private boolean setupOk = false;
    private int recordSize = 10000;
    private String format = "insert into %s values('%s', %d," +
            "100.0, 200.0, 'hello world');";
    private long counter = 0;

    public FESQLLastJoinBenchmark() {
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
        for (int i = 0; i < recordSize/1000; i++) {
            for (int j = 0; j < 1000; j++) {
                String sql = String.format(format, "t1","pk" + i, System.currentTimeMillis());
                executor.executeInsert(db, sql);
                sql = String.format(format, "t2", "pk" + i, System.currentTimeMillis() - j);
                executor.executeInsert(db, sql);
            }
        }
    }

//    @Benchmark
//    public void lastJoinBm() {
//        String sql = "select t1.col1 as c1, t2.col2 as c2 , " +
//                "sum(t1.col3) over w  from t1 last join t2 " +
//                "order by t2.col2 on t1.col1 = t2.col1 and t1.col2 > t2.col2 " +
//                "window w as (partition by t1.col1 order by t1.col2 ROWS Between 1000 preceding and current row);";
//        SQLRequestRow row = executor.getRequestRow(db, sql);
//        String pk = "pk0";
//        row.Init(pk.length());
//        row.AppendString(pk);
//        row.AppendTimestamp(System.currentTimeMillis());
//        row.AppendFloat(1.0f);
//        row.AppendDouble(2.0d);
//        row.AppendNULL();
//        row.Build();
//        executor.executeSQL(db, sql, row);
//    }

    @Benchmark
    public void lastJoinBmWithPreparedStmt() {
        String sql = "select t1.col1 as c1, t2.col2 as c2 , " +
                "sum(t1.col3) over w  from t1 last join t2 " +
                "order by t2.col2 on t1.col1 = t2.col1 and t1.col2 > t2.col2 " +
                "window w as (partition by t1.col1 order by t1.col2 ROWS Between 1000 preceding and current row);";
        PreparedStatement pst = null;
        try {
            pst = executor.getRequestPreparedStmt(db, sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        String pk = "pk0";
        try {
            pst.setString(1, pk);
            pst.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            pst.setFloat(3, 1.0f);
            pst.setDouble(4, 2.0d);
            pst.setNull(5, 0);
            pst.executeQuery();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (pst != null) {
                try {
                    pst.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws RunnerException {

       Options opt = new OptionsBuilder()
                .include(FESQLLastJoinBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
