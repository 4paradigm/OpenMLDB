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
import java.util.*;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(10)
@Fork(value = 1, jvmArgs = {"-Xms8G", "-Xmx8G"})
@Warmup(iterations = 2)
@Measurement(iterations = 5, time = 60)

public class OpenMLDBPerfBenchmark {
    private SqlExecutor executor;
    private String database;
    private String deployName;
    private int windowNum;
    private int windowSize;
    private int joinNum;
    private int unionNum = 0; // unspport in cluster mode in 0.5.0
    private Map<String, TableSchema> tableSchema = new HashMap<>();
    private Random random;
    private List<Integer> pkList = new ArrayList<>();

    public OpenMLDBPerfBenchmark() {
        executor = BenchmarkConfig.GetSqlExecutor(false);
        deployName = BenchmarkConfig.DEPLOY_NAME;
        database = BenchmarkConfig.DATABASE;
        joinNum = BenchmarkConfig.JOIN_NUM;
        windowNum = BenchmarkConfig.WINDOW_NUM;
        windowSize = BenchmarkConfig.WINDOW_SIZE;
        random = new Random(System.currentTimeMillis());
        if (BenchmarkConfig.PK_MAX > 0) {
            for (int i = 0; i < BenchmarkConfig.PK_NUM; i++) {
                int pk = random.nextInt(BenchmarkConfig.PK_MAX);
                if (!pkList.contains(pk)) {
                    pkList.add(pk);
                }
            }
        }
    }

    private void addTableSchema(String dbName, String tableName) {
        NS.TableInfo tableInfo = null;
        try {
            tableInfo = executor.getTableInfo(dbName, tableName);
            TableSchema schema = new TableSchema(tableInfo);
            tableSchema.put(tableName, schema);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void create () {
        Util.executeSQL("CREATE DATABASE IF NOT EXISTS " + database + ";", executor);
        Util.executeSQL("USE " + database + ";", executor);
        String ddl = Util.genDDL("mt", windowNum);
        Util.executeSQL(ddl, executor);
        for (int i = 0; i < unionNum; i++) {
            String tableName = "ut" + String.valueOf(i);
            ddl = Util.genDDL(tableName, windowNum);
            Util.executeSQL(ddl, executor);
        }
        for (int i = 0; i < joinNum; i++) {
            String tableName = "lt" + String.valueOf(i);
            ddl = Util.genDDL(tableName, 1);
            Util.executeSQL(ddl, executor);
        }
    }

    public void putData() {
        Util.putData(pkList, BenchmarkConfig.PK_NUM, tableSchema.get("mt"), windowSize, executor);
        for (int i = 0; i < unionNum; i++) {
            String tableName = "ut" + String.valueOf(i);
            Util.putData(pkList, BenchmarkConfig.PK_NUM, tableSchema.get(tableName), windowSize, executor);
        }
        for (int i = 0; i < joinNum; i++) {
            String tableName = "lt" + String.valueOf(i);
            Util.putData(pkList, BenchmarkConfig.PK_NUM, tableSchema.get(tableName), windowSize, executor);
        }
    }

    public void importData() {
        Util.putData(new ArrayList<>(), BenchmarkConfig.PK_MAX, tableSchema.get("mt"), 1, executor);
        for (int i = 0; i < unionNum; i++) {
            String tableName = "ut" + String.valueOf(i);
            Util.putData(new ArrayList<>(), BenchmarkConfig.PK_MAX, tableSchema.get(tableName), 1, executor);
        }
        for (int i = 0; i < joinNum; i++) {
            String tableName = "lt" + String.valueOf(i);
            Util.putData(new ArrayList<>(), BenchmarkConfig.PK_MAX, tableSchema.get(tableName), 1, executor);
        }
    }

    public void drop() {
        Util.executeSQL("USE " + database + ";", executor);
        Util.executeSQL("DROP DEPLOYMENT " + deployName + ";", executor);
        Util.executeSQL("DROP TABLE mt;", executor);
        for (int i = 0; i < unionNum; i++) {
            Util.executeSQL("DROP TABLE ut" + String.valueOf(i) + ";", executor);
        }
        for (int i = 0; i < joinNum; i++) {
            Util.executeSQL("DROP TABLE lt" + String.valueOf(i) + ";", executor);
        }
        Util.executeSQL("DROP DATABASE " + database + ";", executor);
    }

    public void deploy() {
        String sql = Util.genScript(windowNum, windowSize, unionNum, joinNum);
        System.out.println(sql);
        Util.executeSQL("USE " + database + ";", executor);
        Util.executeSQL("set @@execute_mode='online';", executor);
        Util.executeSQL("DEPLOY " + deployName + " " + sql, executor);
    }

    public void addSchema() {
        addTableSchema(database, "mt");
        for (int i = 0; i < unionNum; i++) {
            String tableName = "ut" + String.valueOf(i);
            addTableSchema(database, tableName);
        }
        for (int i = 0; i < joinNum; i++) {
            String tableName = "lt" + String.valueOf(i);
            addTableSchema(database, tableName);
        }
    }

    @Setup
    public void initEnv() {
        create();
        deploy();
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        addSchema();
        putData();
        if (!pkList.isEmpty()) {
            importData();
        }
    }

    @TearDown
    public void cleanEnv() {
        drop();
    }

    @Benchmark
    public void executeDeployment() {
        int numberKey = BenchmarkConfig.PK_BASE;
        if (pkList.isEmpty()) {
            numberKey += random.nextInt(BenchmarkConfig.PK_NUM);
        } else {
            int pos = random.nextInt(pkList.size());
            numberKey += pkList.get(pos);
        }
        try {
            PreparedStatement stat = null;
            if (BenchmarkConfig.BATCH_SIZE > 0) {
                stat = Util.getBatchPreparedStatement(deployName, numberKey, BenchmarkConfig.BATCH_SIZE, tableSchema.get("mt"), executor);
            } else {
                stat = Util.getPreparedStatement(deployName, numberKey, tableSchema.get("mt"), executor);
            }
            ResultSet resultSet = stat.executeQuery();
            long total = 0;
            while (BenchmarkConfig.PARSE_RESULT && resultSet.next()) {
                Map<String, Object> val = Util.extractResultSet(resultSet);
                total += (long)val.get("sum_w0_col_i1");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        OpenMLDBPerfBenchmark benchmark = new OpenMLDBPerfBenchmark();
        benchmark.initEnv();
        while (true) {
            benchmark.executeDeployment();
        }

        //benchmark.cleanEnv();

        /*try {
            Options opt = new OptionsBuilder()
                    .include(OpenMLDBPerfBenchmark.class.getSimpleName())
                    .forks(1)
                    .build();
            new Runner(opt).run();
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }
}
