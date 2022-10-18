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

import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
//@OutputTimeUnit(TimeUnit.MICROSECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Threads(10)
@Fork(value = 1, jvmArgs = {"-Xms8G", "-Xmx8G"})
@Warmup(iterations = 2)
@Measurement(iterations = 5, time = 60)

public class BenchmarkByBank {
    private SqlExecutor executor;
    private String database;
    private String deployName;
    private Random random = new Random();
    private Map<String, TableSchema> tableSchema = new HashMap<>();


    private List<List<String>> requestList;

    public BenchmarkByBank() {
        executor = BenchmarkConfig.GetSqlExecutor(false);
        deployName = BenchmarkConfig.DEPLOY_NAME;
        database = BenchmarkConfig.DATABASE;
    }

    @Setup
    public void initEnv() {
        requestList = CsvUtil.readCsvByCsvReader(BenchmarkConfig.CSV_PATH);
    }

    @TearDown
    public void cleanEnv() {

    }

    @Benchmark
    public void executeDeployment() {
        List<String> values = requestList.get(random.nextInt(requestList.size()));
        try(CallablePreparedStatement rps = executor.getCallablePreparedStmt(database, deployName)) {
            Util.setRequestData(rps, values);
            ResultSet resultSet = rps.executeQuery();
//            resultSet.next();
//            Map<String, String> val = Util.extractResultSet(resultSet);
//            System.out.println("val = " + val);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        /*OpenMLDBPerfBenchmark benchmark = new OpenMLDBPerfBenchmark();
        benchmark.initEnv();
        benchmark.executeDeployment();
        benchmark.cleanEnv();*/

        try {
            Options opt = new OptionsBuilder()
                    .include(BenchmarkByBank.class.getSimpleName())
                    .forks(1)
                    .build();
            new Runner(opt).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
