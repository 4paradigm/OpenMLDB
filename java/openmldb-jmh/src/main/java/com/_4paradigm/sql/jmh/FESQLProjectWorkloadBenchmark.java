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
import com._4paradigm.openmldb.ResultSet;
import com._4paradigm.openmldb.ScanOption;
import com._4paradigm.openmldb.TableReader;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.Status;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms32G", "-Xmx32G"})
@Warmup(iterations = 1)
public class FESQLProjectWorkloadBenchmark {
    private AtomicLong counter = new AtomicLong(0l);
    private SqlExecutor executor;
    private SdkOption option;
    private String db = "db_insert_benchmark" + System.currentTimeMillis();
    private int recordSize = 10000;
    private String ddl100;
    private String ddl100Insert;

    private String ddl200;
    private String ddl200Insert;

    private String ddl500;
    private String ddl500Insert;

    private String query100 = "select col100 from ddl100 where col98='100_key';";
    private String query200 = "select col200 from ddl200 where col198='200_key';";
    private String query500 = "select col500 from ddl500 where col498='500_key';";
    private String query361 = "select col500 from ddl500 where col498='3000_key'";
    private TableReader reader;
    private long st;
    private long et;

    public FESQLProjectWorkloadBenchmark() {
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
        boolean setupOk = executor.createDB(db);
        if (!setupOk) {
            return;
        }
        StringBuilder ddl100Builder = new StringBuilder();
        StringBuilder ddl100InsertBuilder = new StringBuilder();
        ddl100InsertBuilder.append("insert into ddl100 values(");
        ddl100Builder.append("create table ddl100(");
        for (int i = 0;  i < 99; i++) {
            if (i > 0) {
                ddl100Builder.append(",");
                ddl100InsertBuilder.append(",");
            }
            ddl100Builder.append("col" + String.valueOf(i) + " string");
            ddl100InsertBuilder.append("?");
        }
        ddl100Builder.append(", col99 timestamp, col100 int, index(key=col98, ts=col99)) partitionnum=1;");
        ddl100InsertBuilder.append(", ?, ?);");
        ddl100 = ddl100Builder.toString();
        ddl100Insert = ddl100InsertBuilder.toString();
        setupOk = executor.executeDDL(db, ddl100);
        if (!setupOk) {
            return;
        }
        {
            StringBuilder ddl200Builder = new StringBuilder();
            StringBuilder ddl200InsertBuilder = new StringBuilder();
            ddl200InsertBuilder.append("insert into ddl200 values(");
            ddl200Builder.append("create table ddl200(");
            for (int i = 0;  i < 199; i++) {
                if (i > 0) {
                    ddl200Builder.append(",");
                    ddl200InsertBuilder.append(",");
                }
                ddl200Builder.append("col" + String.valueOf(i) + " string");
                ddl200InsertBuilder.append("?");
            }
            ddl200Builder.append(", col199 timestamp, col200 int, index(key=col198, ts=col199)) partitionnum=1;");
            ddl200InsertBuilder.append(", ?, ?);");
            ddl200 = ddl200Builder.toString();
            ddl200Insert = ddl200InsertBuilder.toString();
            setupOk = executor.executeDDL(db, ddl200);
            if (!setupOk) {
                return;
            }
        }
        {
            StringBuilder ddl500Builder = new StringBuilder();
            StringBuilder ddl500InsertBuilder = new StringBuilder();
            ddl500InsertBuilder.append("insert into ddl500 values(");
            ddl500Builder.append("create table ddl500(");
            for (int i = 0;  i < 499; i++) {
                if (i > 0) {
                    ddl500Builder.append(",");
                    ddl500InsertBuilder.append(",");
                }
                ddl500Builder.append("col" + String.valueOf(i) + " string");
                ddl500InsertBuilder.append("?");
            }
            ddl500Builder.append(", col499 timestamp, col500 int, index(key=col498, ts=col499)) partitionnum=1;");
            ddl500InsertBuilder.append(", ?, ?);");
            ddl500 = ddl500Builder.toString();
            ddl500Insert = ddl500InsertBuilder.toString();
            setupOk = executor.executeDDL(db, ddl500);
            if (!setupOk) {
                return;
            }
        }
        {
            String key = "100_key";
            try {
                for (int a = 0; a < 100; a++) {

                    PreparedStatement impl = executor.getInsertPreparedStmt(db, ddl100Insert);
                    for (int i = 0; i < 98; i++) {
                        impl.setString(i+1, "value10000000000");
                    }
                    impl.setString(99, key);
                    impl.setTimestamp(100, new Timestamp(System.currentTimeMillis()));
                    impl.setInt(101, 10);
                    impl.execute();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        {
            String key = "200_key";
            try {
                for (int a = 0; a < 200; a++) {

                    PreparedStatement impl = executor.getInsertPreparedStmt(db, ddl200Insert);
                    for (int i = 0; i < 198; i++) {
                        impl.setString(i+1, "value10000000000");
                    }
                    impl.setString(199, key);
                    impl.setTimestamp(200, new Timestamp(System.currentTimeMillis()));
                    impl.setInt(201, 10);
                    impl.execute();

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        {
            String key = "500_key";
            try {
                for (int a = 0; a < 500; a++) {

                    PreparedStatement impl = executor.getInsertPreparedStmt(db, ddl500Insert);
                    for (int i = 0; i < 498; i++) {
                        impl.setString(i+1, "value10000000000");
                    }
                    impl.setString(499, key);
                    impl.setTimestamp(500, new Timestamp(System.currentTimeMillis()));
                    impl.setInt(501, 10);
                    impl.execute();

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        {
            String key = "3000_key";
            try {
                long ts = System.currentTimeMillis();
                for (int a = 0; a < 3000; a++) {
                    PreparedStatement impl = executor.getInsertPreparedStmt(db, ddl500Insert);
                    for (int i = 0; i < 498; i++) {
                        impl.setString(i+1, "value10000000000");
                    }
                    impl.setString(499, key);
                    impl.setTimestamp(500, new Timestamp(ts - a));
                    impl.setInt(501, 10);
                    impl.execute();
                }
                query361 += " and col499 > timestamp(" + (ts - 3000) + ") and col499 < timestamp(" +(ts - 2638) +");";
                st = ts - 2638 ;
                et = ts - 3000 ;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            Thread.sleep(2000);
            ResultSet rs = (ResultSet) executor.executeSQL(db, query100);
            if (rs.Size() != 100) {
                throw new Exception("check failed real size " + rs.Size());
            }
            rs = (ResultSet) executor.executeSQL(db, query200);
            if (rs.Size() != 200) {
                throw new Exception("check failed");
            }
            rs = (ResultSet) executor.executeSQL(db, query500);
            if (rs.Size() != 500) {
                throw new Exception("check failed");
            }
            reader = executor.getTableReader();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Benchmark
    public void project100bm() {
        executor.executeSQL(db, query100);
    }

    //@Benchmark
    public void project200bm() {
        executor.executeSQL(db, query200);
    }

   // @Benchmark
    public void project500bm() {
        executor.executeSQL(db, query500);
    }

    //@Benchmark
    public void projectWhere361m() {
        executor.executeSQL(db, query361);
    }

    @Benchmark
    public void projectScan361m() {
        ScanOption so = new ScanOption();
 	Status status = new Status();
        reader.Scan(db, "ddl500", "500_key", st, et, so, status);
    }

    public static void main(String[] args) throws Exception {
        //FESQLProjectWorkloadBenchmark benchmark = new FESQLProjectWorkloadBenchmark();
        //benchmark.setup();
        Options opt = new OptionsBuilder()
                .include(FESQLProjectWorkloadBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
