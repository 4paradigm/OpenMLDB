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

package com._4paradigm.openmldb.benchmark;

import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(10)
@Fork(value = 1, jvmArgs = {"-Xms8G", "-Xmx8G"})
@Warmup(iterations = 2)
@Measurement(iterations = 5, time = 60)
public class OpenMLDBLongWindowBenchmark {
    private SqlExecutor executor;
    private String database;
    @Param ({"demo", "demo_long"})
    private String deployName = "demo_long";
    private int windowSize;
    private int pkNum = 1;
    private Map<String, TableSchema> tableSchema = new HashMap<>();

    public OpenMLDBLongWindowBenchmark() {
        executor = BenchmarkConfig.GetSqlExecutor(false);
        database = BenchmarkConfig.DATABASE;
        windowSize = BenchmarkConfig.WINDOW_SIZE;
    }

    private void addTableSchema(String dbName, String tableName) {
        try {
            NS.TableInfo tableInfo = executor.getTableInfo(dbName, tableName);
            TableSchema schema = new TableSchema(tableInfo);
            tableSchema.put(tableName, schema);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void create () {
        Util.executeSQL("CREATE DATABASE IF NOT EXISTS " + database + ";", executor);
        Util.executeSQL("USE " + database + ";", executor);
        String ddl = Util.genDDL("mt", 1);
        Util.executeSQL(ddl, executor);
    }

    public void putData() {
        Util.putData(new ArrayList<>(), pkNum, tableSchema.get("mt"), windowSize, executor);
    }

    public void drop() {
        Util.executeSQL("USE " + database + ";", executor);
        Util.executeSQL("DROP DEPLOYMENT demo;", executor);
        Util.executeSQL("DROP DEPLOYMENT demo_long;", executor);
        Util.executeSQL("DROP TABLE mt;", executor);
        String aggr_table = "pre_" + database + "_demo_long_w0_count_col_s11"; 
        Util.executeSQL("DROP TABLE __PRE_AGG_DB." + aggr_table + ";", executor);
        Util.executeSQL("DROP DATABASE " + database + ";", executor);
    }

    public void deploy() {
        String sql = "SELECT\n" +
                "col_s0,\n" +
                "count(col_s11) OVER w0 AS count_w0_col_s11,\n" +
                "from mt\n" +
                "window w0 as ( partition by col_s0 order by col_t0 rows_range between 30d PRECEDING AND CURRENT ROW);";
        Util.executeSQL("USE " + database + ";", executor);
        String longWindowDeployment = "DEPLOY demo_long OPTIONS(long_windows=\"w0:1000\") " + sql;
        String normalDeployment = "DEPLOY demo " + sql;
        Util.executeSQL(normalDeployment, executor);
        Util.executeSQL(longWindowDeployment, executor);
    }

    public void addSchema() {
        addTableSchema(database, "mt");
    }

    @Setup
    public void initEnv() {
        if (deployName.equals("demo")) {
            create();
            deploy();
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            addSchema();
            putData();
        } else {
            Util.executeSQL("USE " + database + ";", executor);
            addSchema();
        }
    }

    @TearDown
    public void cleanEnv() {
        if (!deployName.equals("demo")) {
            drop();
        }
    }


    @Benchmark
    public void executeDeployment() {
        int numberKey = BenchmarkConfig.PK_BASE;
        try {
            PreparedStatement stat = Util.getPreparedStatement(deployName, numberKey, tableSchema.get("mt"), executor);
            ResultSet resultSet = stat.executeQuery();
            /*resultSet.next();
            Map<String, String> val = Util.extractResultSet(resultSet);
            int a = 0;*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        /*OpenMLDBLongWindowBenchmark benchmark = new OpenMLDBLongWindowBenchmark();
        benchmark.initEnv();
        benchmark.executeDeployment();
        benchmark.cleanEnv();*/

        try {
            Options opt = new OptionsBuilder()
                    .include(OpenMLDBLongWindowBenchmark.class.getSimpleName())
                    .forks(1)
                    .build();
            new Runner(opt).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
